use std::fmt;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::sync::Mutex;

use crate::config::RuntimeConfig;
use crate::core::raft_msg::external_command::ExternalCommand;
use crate::core::raft_msg::RaftMsg;
use crate::core::TickHandle;
use crate::error::Fatal;
use crate::error::RaftError;
use crate::metrics::RaftDataMetrics;
use crate::metrics::RaftServerMetrics;
use crate::raft::core_state::CoreState;
use crate::type_config::alias::OneshotSenderOf;
use crate::AsyncRuntime;
use crate::Config;
use crate::MessageSummary;
use crate::OptionalSend;
use crate::RaftMetrics;
use crate::RaftTypeConfig;

/// How long a caller waits for `RaftCore` to report a failure after a response channel closed,
/// before concluding the channel was closed by a dropped responder instead.
///
/// A real failure is observable before cleanup: `RaftCore` reports its cause in metrics before
/// joining replication tasks. This only needs to tolerate scheduling latency, not an actual
/// shutdown, and solely bounds how long a caller waits on the dropped-responder path.
const RECV_CORE_STOP_TIMEOUT: Duration = Duration::from_secs(1);

/// RaftInner is the internal handle and provides internally used APIs to communicate with
/// `RaftCore`.
pub(in crate::raft) struct RaftInner<C>
where C: RaftTypeConfig
{
    pub(in crate::raft) id: C::NodeId,
    pub(in crate::raft) config: Arc<Config>,
    pub(in crate::raft) runtime_config: Arc<RuntimeConfig>,
    pub(in crate::raft) tick_handle: TickHandle<C>,
    pub(in crate::raft) tx_api: mpsc::UnboundedSender<RaftMsg<C>>,
    pub(in crate::raft) rx_metrics: watch::Receiver<RaftMetrics<C::NodeId, C::Node>>,
    pub(in crate::raft) rx_data_metrics: watch::Receiver<RaftDataMetrics<C::NodeId>>,
    pub(in crate::raft) rx_server_metrics: watch::Receiver<RaftServerMetrics<C::NodeId, C::Node>>,

    // TODO(xp): it does not need to be a async mutex.
    #[allow(clippy::type_complexity)]
    pub(in crate::raft) tx_shutdown: Mutex<Option<OneshotSenderOf<C, ()>>>,
    pub(in crate::raft) core_state: Mutex<CoreState<C::NodeId, C::AsyncRuntime>>,

    /// The ongoing snapshot transmission.
    pub(in crate::raft) snapshot: Mutex<Option<crate::network::snapshot_transport::Streaming<C>>>,
}

impl<C> RaftInner<C>
where C: RaftTypeConfig
{
    /// Send a RaftMsg to RaftCore
    pub(crate) async fn send_msg(&self, mes: RaftMsg<C>) -> Result<(), Fatal<C::NodeId>> {
        self.rx_metrics.borrow().running_state.clone()?;
        let send_res = self.tx_api.send(mes);

        if let Err(e) = send_res {
            let fatal = self.get_core_stopped_error("sending RaftMsg to RaftCore", Some(e.0.summary())).await;
            return Err(fatal);
        }
        Ok(())
    }

    /// Receive a message from RaftCore, return error if RaftCore has stopped.
    pub(crate) async fn recv_msg<T, E>(&self, rx: impl Future<Output = Result<T, E>>) -> Result<T, Fatal<C::NodeId>>
    where
        T: OptionalSend,
        E: OptionalSend,
    {
        // Prefer a response already delivered before the failure. Otherwise the
        // fatal signal must release callers even when another task still owns
        // their responder and replication cleanup is waiting for a reader.
        let recv_res = tokio::select! {
            biased;
            result = rx => result,
            _ = self.observe_core_failure() => {
                return Err(self.get_core_stopped_error("core failed while awaiting response", None::<u64>).await);
            }
        };
        tracing::debug!("{} receives result is error: {:?}", func_name!(), recv_res.is_err());

        match recv_res {
            Ok(x) => Ok(x),
            Err(_) => {
                let fatal =
                    self.get_core_stopped_error_bounded("receiving rx from RaftCore", None::<&'static str>).await;
                tracing::error!(error = debug(&fatal), "error when {}", func_name!());
                Err(fatal)
            }
        }
    }

    /// Invoke RaftCore by sending a RaftMsg and blocks waiting for response.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn call_core<T, E>(
        &self,
        mes: RaftMsg<C>,
        rx: <C::AsyncRuntime as AsyncRuntime>::OneshotReceiver<Result<T, E>>,
    ) -> Result<T, RaftError<C::NodeId, E>>
    where
        E: Debug + OptionalSend,
        T: OptionalSend,
    {
        self.send_msg(mes).await?;
        self.recv_msg(rx).await?.map_err(RaftError::APIError)
    }

    /// Send an [`ExternalCommand`] to RaftCore to execute in the `RaftCore` thread.
    ///
    /// It returns at once.
    pub(in crate::raft) async fn send_external_command(
        &self,
        cmd: ExternalCommand<C>,
        cmd_desc: impl fmt::Display + Default,
    ) -> Result<(), Fatal<C::NodeId>> {
        self.rx_metrics.borrow().running_state.clone()?;
        let send_res = self.tx_api.send(RaftMsg::ExternalCommand { cmd });

        if send_res.is_err() {
            let fatal = self.get_core_stopped_error("sending external command to RaftCore", Some(cmd_desc)).await;
            return Err(fatal);
        }
        Ok(())
    }

    /// Get the error for a response channel that closed without delivering a reply.
    ///
    /// Usually this means RaftCore has failed, and its error is returned without waiting for
    /// replication cleanup. But a
    /// malfunctioning state machine can drop a responder without replying while RaftCore is still
    /// running, and joining the core then would block forever. The wait is therefore bounded: if
    /// the core has not reported a failure, the dropped responder is itself the failure and
    /// [`Fatal::Stopped`] is returned at once.
    async fn get_core_stopped_error_bounded(
        &self,
        when: impl fmt::Display,
        message_summary: Option<impl fmt::Display + Default>,
    ) -> Fatal<C::NodeId> {
        if self.wait_core_failure(RECV_CORE_STOP_TIMEOUT).await {
            return self.get_core_stopped_error(when, message_summary).await;
        }

        tracing::error!(
            "response dropped without a reply while RaftCore is running, when: {}",
            when
        );
        Fatal::Stopped
    }

    /// Wait up to `timeout` for a RaftCore failure, without joining (and thus consuming) it.
    ///
    /// This observes the metrics watch channel, a non-destructive signal that is safe to poll from
    /// any number of callers: RaftCore sets [`running_state`](RaftMetrics::running_state) to `Err`
    /// before cleanup, and the metrics sender is dropped when its task ends, including on a panic.
    /// Neither observing an error nor returning it to an API caller joins the core task.
    ///
    /// Returns `true` if a failure was observed, `false` if the core is still running after
    /// `timeout`.
    async fn wait_core_failure(&self, timeout: Duration) -> bool {
        C::AsyncRuntime::timeout(timeout, self.observe_core_failure()).await.is_ok()
    }

    /// Observe a failure without requiring the response owner or core task to return.
    async fn observe_core_failure(&self) {
        let mut rx = self.rx_metrics.clone();
        loop {
            if rx.borrow().running_state.is_err() {
                return;
            }
            // A dropped metrics sender also means the core task has ended.
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    /// Get the error that caused RaftCore to stop.
    pub(in crate::raft) async fn get_core_stopped_error(
        &self,
        when: impl fmt::Display,
        message_summary: Option<impl fmt::Display + Default>,
    ) -> Fatal<C::NodeId> {
        // A known fatal cause is available before replication cleanup finishes.
        // Only shutdown needs to join the task; an API error must not wait for readers.
        if let Err(error) = self.rx_metrics.borrow().running_state.clone() {
            tracing::error!(error = debug(&error), "RaftCore failure when {}", when);
            return error;
        }

        // Wait for the core task to finish.
        self.join_core_task().await;

        // Retrieve the result.
        let core_res = {
            let state = self.core_state.lock().await;
            if let CoreState::Done(core_task_res) = &*state {
                core_task_res.clone()
            } else {
                unreachable!("RaftCore should have already quit")
            }
        };

        tracing::error!(
            core_result = debug(&core_res),
            "failure {}; message: {}",
            when,
            message_summary.unwrap_or_default()
        );

        // Safe unwrap: Infallible is unreachable
        core_res.unwrap_err()
    }

    /// Wait for `RaftCore` task to finish and record the returned value from the task.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(in crate::raft) async fn join_core_task(&self) {
        let mut state = self.core_state.lock().await;
        match &mut *state {
            CoreState::Running(handle) => {
                let res = handle.await;
                tracing::info!(res = debug(&res), "RaftCore exited");

                let core_task_res = match res {
                    Err(err) => {
                        if C::AsyncRuntime::is_panic(&err) {
                            Err(Fatal::Panicked)
                        } else {
                            Err(Fatal::Stopped)
                        }
                    }
                    Ok(returned_res) => returned_res,
                };

                *state = CoreState::Done(core_task_res);
            }
            CoreState::Done(_) => {
                // RaftCore has already quit, nothing to do
            }
        }
    }
}
