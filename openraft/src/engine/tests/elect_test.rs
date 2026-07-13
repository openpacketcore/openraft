use std::cell::RefCell;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::future::Future;
use std::io::Cursor;
use std::mem::size_of;
use std::sync::Arc;
use std::time::Duration;

use maplit::btreeset;
use pretty_assertions::assert_eq;
use rand::RngCore;

use crate::core::ServerState;
use crate::engine::testing::UTConfig;
use crate::engine::Command;
use crate::engine::Engine;
use crate::engine::EngineConfig;
use crate::engine::LogIdList;
use crate::raft::VoteRequest;
use crate::testing::log_id;
use crate::utime::UTime;
use crate::AsyncRuntime;
use crate::CommittedLeaderId;
use crate::Config;
use crate::EffectiveMembership;
use crate::LogId;
use crate::Membership;
use crate::OptionalSend;
use crate::RaftTypeConfig;
use crate::TokioInstant;
use crate::TokioRuntime;
use crate::Vote;

fn m1() -> Membership<u64, ()> {
    Membership::new(vec![btreeset! {1}], None)
}

fn m12() -> Membership<u64, ()> {
    Membership::new(vec![btreeset! {1,2}], None)
}

fn eng() -> Engine<UTConfig> {
    let mut eng = Engine::default();
    eng.state.log_ids = LogIdList::new([LogId::new(CommittedLeaderId::new(0, 0), 0)]);
    eng.state.enable_validation(false); // Disable validation for incomplete state
    eng
}

thread_local! {
    static RNG_SCRIPT: RefCell<VecDeque<u64>> = const { RefCell::new(VecDeque::new()) };
}

#[derive(Debug)]
struct ScriptedRng;

impl RngCore for ScriptedRng {
    fn next_u32(&mut self) -> u32 {
        self.next_u64() as u32
    }

    fn next_u64(&mut self) -> u64 {
        RNG_SCRIPT.with(|script| script.borrow_mut().pop_front().expect("scripted election RNG sample"))
    }

    fn fill_bytes(&mut self, destination: &mut [u8]) {
        for chunk in destination.chunks_mut(size_of::<u64>()) {
            let sample = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&sample[..chunk.len()]);
        }
    }

    fn try_fill_bytes(&mut self, destination: &mut [u8]) -> Result<(), rand::Error> {
        self.fill_bytes(destination);
        Ok(())
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct ScriptedRuntime;

impl AsyncRuntime for ScriptedRuntime {
    type JoinError = <TokioRuntime as AsyncRuntime>::JoinError;
    type JoinHandle<T: OptionalSend + 'static> = <TokioRuntime as AsyncRuntime>::JoinHandle<T>;
    type Sleep = <TokioRuntime as AsyncRuntime>::Sleep;
    type Instant = <TokioRuntime as AsyncRuntime>::Instant;
    type TimeoutError = <TokioRuntime as AsyncRuntime>::TimeoutError;
    type Timeout<R, T: Future<Output = R> + OptionalSend> = <TokioRuntime as AsyncRuntime>::Timeout<R, T>;
    type ThreadLocalRng = ScriptedRng;
    type OneshotSender<T: OptionalSend> = <TokioRuntime as AsyncRuntime>::OneshotSender<T>;
    type OneshotReceiverError = <TokioRuntime as AsyncRuntime>::OneshotReceiverError;
    type OneshotReceiver<T: OptionalSend> = <TokioRuntime as AsyncRuntime>::OneshotReceiver<T>;

    fn spawn<T>(future: T) -> Self::JoinHandle<T::Output>
    where
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static,
    {
        TokioRuntime::spawn(future)
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        TokioRuntime::sleep(duration)
    }

    fn sleep_until(deadline: Self::Instant) -> Self::Sleep {
        TokioRuntime::sleep_until(deadline)
    }

    fn timeout<R, F: Future<Output = R> + OptionalSend>(duration: Duration, future: F) -> Self::Timeout<R, F> {
        TokioRuntime::timeout(duration, future)
    }

    fn timeout_at<R, F: Future<Output = R> + OptionalSend>(deadline: Self::Instant, future: F) -> Self::Timeout<R, F> {
        TokioRuntime::timeout_at(deadline, future)
    }

    fn is_panic(join_error: &Self::JoinError) -> bool {
        TokioRuntime::is_panic(join_error)
    }

    fn thread_rng() -> Self::ThreadLocalRng {
        ScriptedRng
    }

    fn oneshot<T>() -> (Self::OneshotSender<T>, Self::OneshotReceiver<T>)
    where T: OptionalSend {
        TokioRuntime::oneshot()
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
struct ScriptedConfig;

impl RaftTypeConfig for ScriptedConfig {
    type D = ();
    type R = ();
    type NodeId = u64;
    type Node = ();
    type Entry = crate::Entry<ScriptedConfig>;
    type SnapshotData = Cursor<Vec<u8>>;
    type AsyncRuntime = ScriptedRuntime;
    type Responder = crate::impls::OneshotResponder<Self>;
}

fn set_rng_script(script: impl IntoIterator<Item = u64>) {
    RNG_SCRIPT.with(|current| {
        let mut current = current.borrow_mut();
        assert!(current.is_empty(), "previous election RNG script was not exhausted");
        current.extend(script);
    });
}

fn assert_rng_script_exhausted() {
    RNG_SCRIPT.with(|script| assert!(script.borrow().is_empty(), "election RNG script was not exhausted"));
}

fn scripted_engine(id: u64, config: EngineConfig<u64>) -> Engine<ScriptedConfig> {
    let mut engine = Engine::new(Default::default(), config);
    engine.state.log_ids = LogIdList::new([LogId::new(CommittedLeaderId::new(0, 0), 0)]);
    engine.state.enable_validation(false);
    engine
        .state
        .membership_state
        .set_effective(Arc::new(EffectiveMembership::new(Some(log_id(0, 1, 1)), m12())));
    engine.config.id = id;
    engine
}

fn election_config() -> Config {
    Config {
        election_timeout_min: 100,
        election_timeout_max: 104,
        ..Default::default()
    }
}

#[test]
fn test_default_election_timer_bounds_are_coherent() {
    let config = EngineConfig::<u64>::default();
    let minimum = Duration::from_millis(config.election_timeout_min);
    let maximum = Duration::from_millis(config.election_timeout_max);

    assert!(minimum <= config.timer_config.election_timeout);
    assert!(config.timer_config.election_timeout < maximum);
    assert_eq!(maximum, config.timer_config.leader_lease);
    assert!(config.timer_config.smaller_log_timeout > maximum);
}

#[test]
fn test_election_timeout_is_resampled_for_every_campaign() {
    set_rng_script([0, 1_u64 << 62, 1_u64 << 63, 3_u64 << 62]);

    let config = EngineConfig::new::<ScriptedRuntime>(1, &election_config());
    let leader_lease = config.timer_config.leader_lease;
    let smaller_log_timeout = config.timer_config.smaller_log_timeout;
    assert_eq!(Duration::from_millis(100), config.timer_config.election_timeout);
    assert_eq!(Duration::from_millis(104), leader_lease);
    assert_eq!(Duration::from_millis(208), smaller_log_timeout);

    let mut engine = scripted_engine(1, config);
    for expected_timeout in [101, 102, 103] {
        engine.elect();
        let election_timeout = engine.config.timer_config.election_timeout;
        assert_eq!(Duration::from_millis(expected_timeout), election_timeout);
        assert!((Duration::from_millis(100)..Duration::from_millis(104)).contains(&election_timeout));
        assert_eq!(leader_lease, engine.config.timer_config.leader_lease);
        assert_eq!(smaller_log_timeout, engine.config.timer_config.smaller_log_timeout);
    }
    assert_eq!(Vote::new(3, 1), *engine.state.vote_ref());
    assert_rng_script_exhausted();
}

#[test]
fn test_repeated_tied_campaigns_resample_to_different_timeouts() {
    set_rng_script([0, 0, 1_u64 << 62, 3_u64 << 62, 1_u64 << 63, 0]);

    let mut one = scripted_engine(1, EngineConfig::new::<ScriptedRuntime>(1, &election_config()));
    let mut two = scripted_engine(2, EngineConfig::new::<ScriptedRuntime>(2, &election_config()));
    assert_eq!(
        one.config.timer_config.election_timeout,
        two.config.timer_config.election_timeout
    );

    one.elect();
    two.elect();
    assert_eq!(Vote::new(1, 1), *one.state.vote_ref());
    assert_eq!(Vote::new(1, 2), *two.state.vote_ref());
    assert_eq!(Duration::from_millis(101), one.config.timer_config.election_timeout);
    assert_eq!(Duration::from_millis(103), two.config.timer_config.election_timeout);

    one.elect();
    two.elect();
    assert_eq!(Vote::new(2, 1), *one.state.vote_ref());
    assert_eq!(Vote::new(2, 2), *two.state.vote_ref());
    assert_eq!(Duration::from_millis(102), one.config.timer_config.election_timeout);
    assert_eq!(Duration::from_millis(100), two.config.timer_config.election_timeout);
    assert_rng_script_exhausted();
}

#[test]
fn test_elect_single_node() -> anyhow::Result<()> {
    tracing::info!("--- single node: still need to wait for Vote to persist");
    {
        let mut eng = eng();
        eng.config.id = 1;
        eng.state
            .membership_state
            .set_effective(Arc::new(EffectiveMembership::new(Some(log_id(0, 1, 1)), m1())));

        eng.elect();

        assert_eq!(Vote::new(1, 1), *eng.state.vote_ref());
        assert!(eng.leader.is_none());
        assert!(eng.candidate_ref().is_some(), "candidate state is pending");

        assert_eq!(ServerState::Candidate, eng.state.server_state);

        assert_eq!(
            vec![
                //
                Command::SaveVote { vote: Vote::new(1, 1) },
                Command::SendVote {
                    vote_req: VoteRequest {
                        vote: Vote::new(1, 1),
                        last_log_id: Some(log_id(0, 0, 0)),
                    },
                },
            ],
            eng.output.take_commands()
        );
    }

    Ok(())
}

#[test]
fn test_elect_single_node_elect_again() -> anyhow::Result<()> {
    tracing::info!("--- single node: electing again will override previous state");
    {
        let mut eng = eng();
        eng.config.id = 1;
        eng.state
            .membership_state
            .set_effective(Arc::new(EffectiveMembership::new(Some(log_id(0, 1, 1)), m1())));

        // Build in-progress election state
        eng.state.vote = UTime::new(TokioInstant::now(), Vote::new_committed(1, 2));
        eng.testing_new_leader();
        eng.candidate_mut().map(|candidate| candidate.grant_by(&1));

        eng.elect();

        assert_eq!(Vote::new(2, 1), *eng.state.vote_ref());
        assert_eq!(Vote::new(2, 1), *eng.candidate_ref().unwrap().vote_ref());

        assert!(eng.candidate_mut().is_some(), "candidate state is pending");

        assert_eq!(ServerState::Candidate, eng.state.server_state);

        assert_eq!(
            vec![
                //
                Command::SaveVote { vote: Vote::new(2, 1) },
                Command::SendVote {
                    vote_req: VoteRequest {
                        vote: Vote::new(2, 1),
                        last_log_id: Some(log_id(0, 0, 0)),
                    },
                },
            ],
            eng.output.take_commands()
        );
    }
    Ok(())
}

#[test]
fn test_elect_multi_node_enter_candidate() -> anyhow::Result<()> {
    tracing::info!("--- multi nodes: enter candidate state");
    {
        let mut eng = eng();
        eng.config.id = 1;
        eng.state
            .membership_state
            .set_effective(Arc::new(EffectiveMembership::new(Some(log_id(0, 1, 1)), m12())));
        eng.state.log_ids = LogIdList::new(vec![log_id(1, 1, 1)]);

        eng.elect();

        assert_eq!(Vote::new(1, 1), *eng.state.vote_ref());
        assert!(eng.leader.is_none());
        assert_eq!(Vote::new(1, 1), *eng.candidate_ref().unwrap().vote_ref());

        assert_eq!(
            Some(btreeset! {},),
            eng.candidate_ref().map(|x| x.granters().collect::<BTreeSet<_>>())
        );

        assert_eq!(ServerState::Candidate, eng.state.server_state);

        assert_eq!(
            vec![
                //
                Command::SaveVote { vote: Vote::new(1, 1) },
                Command::SendVote {
                    vote_req: VoteRequest::new(Vote::new(1, 1), Some(log_id(1, 1, 1)))
                },
            ],
            eng.output.take_commands()
        );
    }
    Ok(())
}
