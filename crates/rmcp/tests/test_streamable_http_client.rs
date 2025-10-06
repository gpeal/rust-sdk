#![cfg(all(feature = "client", feature = "transport-streamable-http-client"))]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::streamable_http::{
    MockClientError, MockStreamableHttpClient, TestSseStream, empty_sse_stream,
    json_notification_stream, sample_initialize_request, sample_initialize_response,
    sample_initialized_notification, sample_notification_message, wait_for_delete_calls,
    wait_for_get_stream_calls,
};
use futures::stream;
use rmcp::model::ServerJsonRpcMessage;
use rmcp::transport::common::client_side_sse::{ExponentialBackoff, NeverRetry};
use rmcp::transport::streamable_http_client::{
    StreamableHttpClientTransportConfig, StreamableHttpClientWorker, StreamableHttpError,
    StreamableHttpPostResponse,
};
use rmcp::transport::worker::{Worker, WorkerContext, WorkerQuitReason, WorkerSendRequest};
use serde_json;
use sse_stream::{Error as SseError, Sse};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const WAIT_DURATION: Duration = Duration::from_secs(1);
const AUTH_RECONNECT: &str = "Bearer reconnect";

type ClientWorker = StreamableHttpClientWorker<MockStreamableHttpClient>;
type WorkerResult = Result<(), WorkerQuitReason<StreamableHttpError<MockClientError>>>;
type WorkerHandle = JoinHandle<WorkerResult>;

#[tokio::test]
async fn worker_uses_auth_header_for_initial_stream() {
    let mock_client = MockStreamableHttpClient::new();
    let auth_value = "Bearer test-token";

    mock_client
        .enqueue_post_response(Ok(sample_initialize_response()))
        .await;
    mock_client
        .enqueue_post_response(Ok(StreamableHttpPostResponse::Accepted))
        .await;
    mock_client
        .enqueue_get_stream_response(Ok(empty_sse_stream()))
        .await;
    mock_client.enqueue_delete_response(Ok(())).await;

    let config = StreamableHttpClientTransportConfig {
        uri: Arc::<str>::from("http://example.com"),
        retry_config: Arc::new(NeverRetry),
        channel_buffer_capacity: 8,
        allow_stateless: false,
        auth_header: Some(auth_value.to_string()),
    };

    let worker = StreamableHttpClientWorker::new(mock_client.clone(), config);
    let (mut handler_rx, to_worker_tx, cancellation_token, worker_handle) =
        spawn_worker(worker).await;

    send_initialize_sequence(&to_worker_tx).await;
    let response = handler_rx.recv().await.expect("initialize response");
    assert!(matches!(response, ServerJsonRpcMessage::Response(_)));

    send_initialized_notification(&to_worker_tx).await;

    wait_for_get_stream_calls(&mock_client, 1, WAIT_DURATION).await;
    let calls = mock_client.get_stream_calls().await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].auth_header.as_deref(), Some(auth_value));
    assert_eq!(calls[0].session_id.as_ref(), "session-1");

    cancellation_token.cancel();
    wait_for_delete_calls(&mock_client, 1, WAIT_DURATION).await;
    let result = worker_handle.await.expect("worker join");
    assert!(matches!(result, Err(WorkerQuitReason::Cancelled)));
}

#[tokio::test]
async fn reconnect_reuses_auth_header_on_retry() {
    let mock_client = MockStreamableHttpClient::new();

    mock_client
        .enqueue_post_response(Ok(sample_initialize_response()))
        .await;
    mock_client
        .enqueue_post_response(Ok(StreamableHttpPostResponse::Accepted))
        .await;

    let first_notification = sample_notification_message();
    let reconnect_notification = sample_notification_message();

    let initial_stream = stream_with_event_and_error(first_notification.clone(), "last-event");
    mock_client
        .enqueue_get_stream_response(Ok(initial_stream))
        .await;
    mock_client
        .enqueue_get_stream_response(Ok(json_notification_stream(&reconnect_notification)))
        .await;
    mock_client.enqueue_delete_response(Ok(())).await;

    let config = StreamableHttpClientTransportConfig {
        uri: Arc::<str>::from("http://example.com"),
        retry_config: Arc::new(ExponentialBackoff {
            max_times: Some(2),
            base_duration: Duration::from_millis(10),
        }),
        channel_buffer_capacity: 8,
        allow_stateless: false,
        auth_header: Some(AUTH_RECONNECT.to_string()),
    };

    let worker = StreamableHttpClientWorker::new(mock_client.clone(), config);
    let (mut handler_rx, to_worker_tx, cancellation_token, worker_handle) =
        spawn_worker(worker).await;

    send_initialize_sequence(&to_worker_tx).await;
    handler_rx.recv().await.expect("initialize response");
    send_initialized_notification(&to_worker_tx).await;

    let first_stream_message = handler_rx.recv().await.expect("first stream message");
    assert_message_eq(&first_stream_message, &first_notification);

    let reconnect_message = handler_rx.recv().await.expect("reconnect message");
    assert_message_eq(&reconnect_message, &reconnect_notification);

    wait_for_get_stream_calls(&mock_client, 2, WAIT_DURATION).await;
    let calls = mock_client.get_stream_calls().await;
    assert_eq!(calls.len(), 2);
    for call in &calls {
        assert_eq!(call.auth_header.as_deref(), Some(AUTH_RECONNECT));
    }
    assert_eq!(calls[1].last_event_id.as_deref(), Some("last-event"));

    cancellation_token.cancel();
    wait_for_delete_calls(&mock_client, 1, WAIT_DURATION).await;
    let result = worker_handle.await.expect("worker join");
    assert!(matches!(result, Err(WorkerQuitReason::Cancelled)));
}

async fn send_initialize_sequence(tx: &mpsc::Sender<WorkerSendRequest<ClientWorker>>) {
    let (init_ack_tx, init_ack_rx) = oneshot::channel();
    tx.send(WorkerSendRequest {
        message: sample_initialize_request(),
        responder: init_ack_tx,
    })
    .await
    .expect("send initialize");
    init_ack_rx
        .await
        .expect("initialize ack send")
        .expect("initialize ok");
}

async fn send_initialized_notification(tx: &mpsc::Sender<WorkerSendRequest<ClientWorker>>) {
    let (ready_ack_tx, ready_ack_rx) = oneshot::channel();
    tx.send(WorkerSendRequest {
        message: sample_initialized_notification(),
        responder: ready_ack_tx,
    })
    .await
    .expect("send initialized notification");
    ready_ack_rx
        .await
        .expect("initialized ack send")
        .expect("initialized ok");
}

async fn spawn_worker(
    worker: ClientWorker,
) -> (
    mpsc::Receiver<ServerJsonRpcMessage>,
    mpsc::Sender<WorkerSendRequest<ClientWorker>>,
    CancellationToken,
    WorkerHandle,
) {
    let channel_capacity = 8;
    let (to_handler_tx, handler_rx) = mpsc::channel::<ServerJsonRpcMessage>(channel_capacity);
    let (to_worker_tx, from_handler_rx) =
        mpsc::channel::<WorkerSendRequest<ClientWorker>>(channel_capacity);
    let cancellation_token = CancellationToken::new();
    let context = WorkerContext {
        to_handler_tx,
        from_handler_rx,
        cancellation_token: cancellation_token.clone(),
    };
    let handle = tokio::spawn(worker.run(context));
    (handler_rx, to_worker_tx, cancellation_token, handle)
}

fn stream_with_event_and_error(message: ServerJsonRpcMessage, event_id: &str) -> TestSseStream {
    let data = serde_json::to_string(&message).expect("serialize message");
    let event = Sse::default().id(event_id).data(data);
    Box::pin(stream::iter(vec![Ok(event), Err(SseError::InvalidLine)]))
}

fn assert_message_eq(actual: &ServerJsonRpcMessage, expected: &ServerJsonRpcMessage) {
    let actual_value = serde_json::to_value(actual).expect("serialize actual");
    let expected_value = serde_json::to_value(expected).expect("serialize expected");
    assert_eq!(actual_value, expected_value);
}
