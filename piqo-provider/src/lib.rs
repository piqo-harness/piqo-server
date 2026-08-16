//! Provider transport and verbatim request-body composition.

mod body;
mod transport;

pub use body::{merge_request_bodies, BodyMergeError};
pub use transport::{
    parse_non_stream_response, parse_sse_event, ProviderDelta, ProviderProtocol, ProviderTransport,
    ProviderTransportError,
};
