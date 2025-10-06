use std::{collections::VecDeque, sync::Arc, time::Duration};

use futures::stream::{self, BoxStream};
use rmcp::{
    model::{
        ClientCapabilities, ClientJsonRpcMessage, ClientNotification, ClientRequest,
        Implementation, InitializeRequest, InitializeRequestParam, InitializedNotification,
        NumberOrString, ProtocolVersion, ServerInfo, ServerJsonRpcMessage, ServerNotification,
        ServerResult,
    },
    transport::streamable_http_client::{
        StreamableHttpClient, StreamableHttpError, StreamableHttpPostResponse,
    },
};
use sse_stream::{Error as SseError, Sse};
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep};

pub type TestSseStream = BoxStream<'static, Result<Sse, SseError>>;

#[derive(Clone, Default)]
pub struct MockStreamableHttpClient {
    state: Arc<Mutex<MockClientState>>,
}

#[derive(Default)]
struct MockClientState {
    post_responses:
        VecDeque<Result<StreamableHttpPostResponse, StreamableHttpError<MockClientError>>>,
    get_stream_calls: Vec<GetStreamCall>,
    get_stream_responses: VecDeque<Result<TestSseStream, StreamableHttpError<MockClientError>>>,
    delete_call_count: usize,
    delete_responses: VecDeque<Result<(), StreamableHttpError<MockClientError>>>,
}

#[derive(Debug, Clone)]
pub struct GetStreamCall {
    pub session_id: Arc<str>,
    pub last_event_id: Option<String>,
    pub auth_header: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MockClientError;

impl std::fmt::Display for MockClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock client error")
    }
}

impl std::error::Error for MockClientError {}

impl MockStreamableHttpClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn enqueue_post_response(
        &self,
        response: Result<StreamableHttpPostResponse, StreamableHttpError<MockClientError>>,
    ) {
        let mut state = self.state.lock().await;
        state.post_responses.push_back(response);
    }

    pub async fn enqueue_get_stream_response(
        &self,
        response: Result<TestSseStream, StreamableHttpError<MockClientError>>,
    ) {
        let mut state = self.state.lock().await;
        state.get_stream_responses.push_back(response);
    }

    pub async fn enqueue_delete_response(
        &self,
        response: Result<(), StreamableHttpError<MockClientError>>,
    ) {
        let mut state = self.state.lock().await;
        state.delete_responses.push_back(response);
    }

    pub async fn get_stream_calls(&self) -> Vec<GetStreamCall> {
        let state = self.state.lock().await;
        state.get_stream_calls.clone()
    }

    pub async fn delete_call_count(&self) -> usize {
        let state = self.state.lock().await;
        state.delete_call_count
    }
}

impl StreamableHttpClient for MockStreamableHttpClient {
    type Error = MockClientError;

    fn post_message(
        &self,
        _uri: Arc<str>,
        _message: ClientJsonRpcMessage,
        _session_id: Option<Arc<str>>,
        _auth_header: Option<String>,
    ) -> impl std::future::Future<
        Output = Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>>,
    > + Send
    + '_ {
        let state = self.state.clone();
        async move {
            let mut guard = state.lock().await;
            guard
                .post_responses
                .pop_front()
                .expect("missing mock post response")
        }
    }

    fn delete_session(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        auth_header: Option<String>,
    ) -> impl std::future::Future<Output = Result<(), StreamableHttpError<Self::Error>>> + Send + '_
    {
        let state = self.state.clone();
        async move {
            let mut guard = state.lock().await;
            let _ = (uri, session_id, auth_header);
            guard.delete_call_count += 1;
            guard
                .delete_responses
                .pop_front()
                .expect("missing mock delete response")
        }
    }

    fn get_stream(
        &self,
        _uri: Arc<str>,
        session_id: Arc<str>,
        last_event_id: Option<String>,
        auth_header: Option<String>,
    ) -> impl std::future::Future<Output = Result<TestSseStream, StreamableHttpError<Self::Error>>>
    + Send
    + '_ {
        let state = self.state.clone();
        async move {
            let mut guard = state.lock().await;
            guard.get_stream_calls.push(GetStreamCall {
                session_id: Arc::clone(&session_id),
                last_event_id: last_event_id.clone(),
                auth_header: auth_header.clone(),
            });
            guard
                .get_stream_responses
                .pop_front()
                .expect("missing mock get_stream response")
        }
    }
}

pub async fn wait_for_get_stream_calls(
    client: &MockStreamableHttpClient,
    expected: usize,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        if client.get_stream_calls().await.len() >= expected {
            break;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for get_stream calls (expected {expected})");
        }
        sleep(Duration::from_millis(10)).await;
    }
}

pub async fn wait_for_delete_calls(
    client: &MockStreamableHttpClient,
    expected: usize,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        if client.delete_call_count().await >= expected {
            break;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for delete_session calls (expected {expected})");
        }
        sleep(Duration::from_millis(10)).await;
    }
}

pub fn empty_sse_stream() -> TestSseStream {
    Box::pin(stream::empty())
}

pub fn json_notification_stream(message: &ServerJsonRpcMessage) -> TestSseStream {
    let data = serde_json::to_string(message).expect("serialize message");
    let sse = Sse::default().data(data);
    Box::pin(stream::once(async move { Ok(sse) }))
}

pub fn sample_initialize_request() -> ClientJsonRpcMessage {
    let params = InitializeRequestParam {
        protocol_version: ProtocolVersion::default(),
        capabilities: ClientCapabilities::default(),
        client_info: Implementation::from_build_env(),
    };
    let request = ClientRequest::InitializeRequest(InitializeRequest::new(params));
    ClientJsonRpcMessage::request(request, NumberOrString::Number(1))
}

pub fn sample_initialized_notification() -> ClientJsonRpcMessage {
    let notification =
        ClientNotification::InitializedNotification(InitializedNotification::default());
    ClientJsonRpcMessage::notification(notification)
}

pub fn sample_initialize_response() -> StreamableHttpPostResponse {
    StreamableHttpPostResponse::Json(
        ServerJsonRpcMessage::response(
            ServerResult::InitializeResult(ServerInfo::default()),
            NumberOrString::Number(1),
        ),
        Some("session-1".to_string()),
    )
}

pub fn sample_notification_message() -> ServerJsonRpcMessage {
    ServerJsonRpcMessage::notification(ServerNotification::ToolListChangedNotification(
        Default::default(),
    ))
}
