# openraft-memstore

This is an in-memory example storage implementation based on [openraft](https://github.com/datafuselabs/openraft/).

Enable this crate's `storage-v2` feature to implement `RaftLogStorage` and
`RaftStateMachine` directly on `Arc<MemStore>`. The default configuration retains
the legacy `RaftStorage` implementation. Both APIs use the same storage operations
and fault-injection controls.

From the workspace root, `make test_storage_v2` runs the engine, the storage
contract suite, and the integration tests with `serde,storage-v2,single-term-leader`.
This configuration is also included in `make test` and the regular CI matrix.

This crate is built mainly for testing or demonstrating purpose.:)
