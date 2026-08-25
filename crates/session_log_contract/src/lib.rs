#![forbid(unsafe_code)]

mod checkpoint;
pub mod client;
mod endpoint;
mod protocol;

pub use checkpoint::{CheckpointType, CommandCheckpoint};
pub use endpoint::ServiceEndpoint;
pub use protocol::{
    ActivateRuntimeLeaseRequest, AppendSessionFeedEventRequest, CommitRuntimeEventRequest,
    ContextSlice, CreateSessionRequest, DeleteSessionRequest, DeleteWorkspaceRequest,
    ExecuteSessionCommandRequest, GetRuntimeLeaseRequest, GetSessionRequest,
    ListSessionRecordsRequest, ListSessionsRequest, MarkSessionInterruptedRequest, Page,
    PersistSessionDeltaRequest, ReadContextSliceRequest, ReadSessionFeedRequest,
    RecoveryCloseRuntimeOutcome, RecoveryCloseRuntimeReason, RecoveryCloseRuntimeRequest,
    RegisterRuntimeRequest, ReplayRuntimeRequest, RuntimeEventCommitOutcome, RuntimeLeaseOutcome,
    RuntimeLeaseSnapshot, RuntimeRecoveryQuiescenceProof, RuntimeRecoveryReceipt,
    RuntimeRegistrationOutcome, RuntimeReplay, SessionCommandResult, SessionContextRecord,
    SessionDeltaEntry, SessionFeedAppendOutcome, SessionFeedCommandUpdate, SessionFeedEntry,
    SessionFeedEvent, SessionLogCommand, SessionLogResponse, SessionMetadata, SessionMetadataPatch,
    SessionRecord, SessionRecordProjection, SessionSnapshot, SessionSummary, UpdateSessionRequest,
    UpdateSessionTodosRequest, WorkspaceSummary,
};
