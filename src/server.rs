use axum::{
    BoxError,
    extract::{State, Path, Json},
    extract::ws::{WebSocketUpgrade, WebSocket, Message as WsMessage},
    extract::connect_info::ConnectInfo,
    http::StatusCode,
    error_handling::HandleErrorLayer,
    response::{Response, IntoResponse},
    routing::{get, post},
    Router
};
use std::{sync::Arc, net::{SocketAddr, IpAddr}, collections::HashMap};
use std::time::Duration;
use anyhow::{Result, bail};
use tokio::time::sleep;
use tokio::sync::Mutex;
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use tracing::{error, debug};
use serde::Serialize;
use tower::ServiceBuilder;

use super::nodes::ClusterNode;
use super::storage::OPAX_RESERVED_KEYS;
use super::opax::{OpaxResponse, OpaxRequest, OpaxMessage, OpaxEntry, OpaxEntryKey,
    OpaxQuery};


#[derive(Clone)]
pub struct PeerNode {
    addr: SocketAddr,
    // secret: String, // TODO: all peers should authenticate
}

#[derive(Clone)]
pub struct HttpServer {
    pub node_id: u64,
    pub peers: HashMap<u64, PeerNode>,
    pub clients: Vec<IpAddr>,
    pub cache_life: Duration,
    inbox: Arc<Mutex<Vec<OpaxMessage>>>,
    responses: Arc<Mutex<Vec<OpaxQuery>>>
}

impl HttpServer {
    /// Returns all messages received since last called.
    pub async fn get_received(&mut self) -> Vec<OpaxMessage> {
        let mut unprocessed = self.inbox.lock().await;
        
        let messages = unprocessed.to_vec();
        unprocessed.clear();
        
        messages
    }
    /// Check if response can be returned from the cache
    pub async fn get_cached(&mut self, req: &OpaxRequest) -> Option<OpaxQuery> {
        let cache = self.responses.lock().await;

        cache.iter().find(|q|q.request.eq(req)).cloned()
            .and_then(|q| match q.is_older_than(&self.cache_life) {
                true => None,
                false => Some(q),
            })
    }
    pub async fn wipe_cache(&mut self, req: &OpaxRequest, resp: &OpaxResponse) {
        let mut cache = self.responses.lock().await;

        // Keys are of interest here. If key is deleted or its value modified we should
        // wipe get responses along with the existing put / delete response.
        match req {
            OpaxRequest::Delete(k) => cache.retain(|q| match q.request {
                // Keep delete query matching request and response, delete others
                // where delete request key matches the given request's key
                OpaxRequest::Delete(ref k) => match q.request.eq(req) {
                    true => match q.response.eq(resp) {
                        true => true,
                        false => q.request.key().ne(k),
                    },
                    false => true,
                },
                // If key is deleted get rid of cached get and put responses
                // pointing at the deleted key
                OpaxRequest::Get(ref s) => s.ne(k),
                OpaxRequest::Put(ref e) => e.key.ne(k),
                _ => true,
            }),
            OpaxRequest::Put(e) => cache.retain(|q| match q.request {
                // As above, only targeting cached put responses instead of delete
                OpaxRequest::Put(ref e) => match q.request.eq(req) {
                    true => match q.response.eq(resp) {
                        true => true,
                        false => q.request.key().ne(&e.key),
                    },
                    false => true,
                },
                // Also as above but get rid of cached delete responses
                OpaxRequest::Get(ref s) => s.ne(&e.key),
                OpaxRequest::Delete(ref s) => s.ne(&e.key),
                _ => true,
            }),
            x => panic!("Stupid developer only wipe cache on put or \
                delete DB actions. You're feeding request of type {:?}", x),
        }
    }
    pub async fn receiver(&self, message: OpaxMessage) {
        match message {
            // API response is added to the cache if it's not there already. Requests
            // that are identical produce identical responses.
            OpaxMessage::APIResponse(ref q) => {
                let mut responses = self.responses.lock().await;
                
                match responses.iter().enumerate().find(|(_, i)|i.request.eq(&q.request)) {
                    Some((i, x)) => match x.is_older_than(&self.cache_life) {
                        true => {
                            responses.remove(i);
                            responses.push(q.to_owned());

                            debug!("Reloading API response for {} to cache", q.request);
                        },
                        false => {
                            debug!("Loading API response for {} from cache", q.request);
                        }
                    },
                    None => {
                        debug!("Storing API response for {} to cache", q.request);
                        responses.push(q.to_owned());
                    }
                }

                return
            },
            _ => (),
        }

        self.inbox.lock().await.push(message);
    }
    pub async fn new(node: ClusterNode, peers: &HashMap<u64, String>) -> Result<Self> {
        let ip = node.addr.parse::<SocketAddr>()?;
        let messages = Arc::new(Mutex::new(vec![]));
        let inbox = messages.clone();

        let api_response = Arc::new(Mutex::new(vec![]));
        let responses = api_response.clone();

        let peers = peers.iter()
            .map(|(i, s)|(*i, PeerNode { addr: s.parse().unwrap() }))
            .collect::<HashMap<u64, PeerNode>>();

        let clients = match node.clients {
            Some(v) => v.clone().into_iter()
                .map(|s|s.parse().unwrap())
                .collect::<Vec<IpAddr>>(),
            None => vec!["127.0.0.1".parse().unwrap()],
        };

        let server = Self { node_id: node.id, inbox, responses, peers,
            clients, cache_life: node.cache };
        let state = server.clone();

        tokio::spawn(async move {
            // Build our application by composing routes
            let app = Router::new()
                .route(
                    "/omnipax",
                    get(peer_ws_handler))
                .route(
                    "/client",
                    get(client_ws_handler))
                .route(
                    "/key",
                    post(write_handler))
                .route(
                    "/key/:key",
                    get(read_handler).delete(destroy_handler))
                .route(
                    "/keys",
                    get(multi_read_handler).post(multi_write_handler).delete(multi_destroy_handler))
                .route(
                    "/ping",
                    get(ping_handler))
                .layer(
                    ServiceBuilder::new()
                        // `timeout` will produce an error if the handler takes
                        // too long so we must handle those
                        .layer(HandleErrorLayer::new(handle_timeout_error))
                        .timeout(Duration::from_secs(5))
                )
                .with_state(state);

            let server = axum::Server::bind(&ip)
                .serve(app
                    .into_make_service_with_connect_info::<SocketAddr>()
                );

            match server.await {
                Ok(_) => Ok(()),
                Err(e) => bail!("Failed to run server: {}", e)
            }
        });

        Ok(server)
    }
}

async fn handle_timeout_error(err: BoxError) -> (StatusCode, String) {
    if err.is::<tower::timeout::error::Elapsed>() {
        (StatusCode::REQUEST_TIMEOUT, "Request took too long".to_string())
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("Unhandled internal error: {}", err))
    }
}

// TODO: should be secure socket (wss) if ever used outside of private network
async fn peer_ws_handler(ConnectInfo(addr): ConnectInfo<SocketAddr>,
ws: WebSocketUpgrade, state: State<HttpServer>) -> Response {
    if state.peers.values().find(|p|p.addr.ip().eq(&addr.ip())).is_none() {
        return (
            StatusCode::FORBIDDEN,
            "Unknown peer node. Access denied."
        ).into_response()
    }
    
    ws.on_upgrade(|socket| peer_ws(socket, state))
}

// TODO: as above
async fn client_ws_handler(ConnectInfo(addr): ConnectInfo<SocketAddr>,
ws: WebSocketUpgrade, state: State<HttpServer>) -> Response {
    if ! state.clients.is_empty() && state.clients.iter().find(|i|i.eq(&&addr.ip())).is_none() {
        return (
            StatusCode::FORBIDDEN,
            "Client IP not on the permitted clients list. Access denied."
        ).into_response()
    }
    
    ws.on_upgrade(|socket| client_ws(socket, state))
}

async fn peer_ws(socket: WebSocket, state: State<HttpServer>) {
    let (_, mut receiver) = socket.split();
    let inbox = state.inbox.clone();

    while let Some(r) = receiver.next().await {
        let message = match r {
            Ok(m) => m,
            Err(e) => {
                error!("Failed to read incoming message: {}", e);
                continue;
            }
        };
        
        match message {
            WsMessage::Text(s) => match serde_json::from_str::<OpaxMessage>(&s) {
                Ok(m) => { inbox.lock().await.push(m); },
                Err(e) => error!("Unable to deserialize string message: {}", e),
            }
            WsMessage::Binary(v) => match serde_json::from_slice::<OpaxMessage>(&v) {
                Ok(m) => { inbox.lock().await.push(m); },
                Err(e) => error!("Unable to deserialize binary message: {}", e),
            }
            WsMessage::Ping(v) => {
                debug!("Received ping request: {}", String::from_utf8_lossy(&v));
                continue
            },
            // Left as a reminder.
            WsMessage::Pong(_v) => continue,
            WsMessage::Close(_o) => continue,
        }
    }
}

#[derive(Serialize)]
struct StreamResp {
    #[serde(skip_serializing_if = "Option::is_none")]
    request: Option<OpaxRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response: Option<OpaxResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>
}

async fn client_ws(socket: WebSocket, state: State<HttpServer>) {
    let (mut sender, mut receiver) = socket.split();
    let inbox = state.inbox.clone();

    while let Some(r) = receiver.next().await {
        let message = match r {
            Ok(m) => m,
            Err(e) => {
                error!("Failed to read incoming message: {}", e);
                continue;
            }
        };

        let mut resp = StreamResp {
            request: None, response: None, error: None
        };
        
        let opax_result = match message {
            WsMessage::Text(s) => match serde_json::from_str::<OpaxMessage>(&s) {
                Ok(m) => Ok(m),
                Err(e) => Err(format!("Unable to deserialize string message: {}", e)),
            }
            WsMessage::Binary(v) => match serde_json::from_slice::<OpaxMessage>(&v) {
                Ok(m) => Ok(m),
                Err(e) => Err(format!("Unable to deserialize binary message: {}", e)),
            }
            _ => continue,
        };

        let opax_message = match opax_result {
            Ok(m) => m,
            Err(e) => {
                error!("Server failed to read incoming message: {}", &e);
                resp.error = Some(e);

                let item = match serde_json::to_string(&resp) {
                    Ok(s) => WsMessage::Text(s),
                    Err(e) => {
                        error!("Failed to serialize websocket unserializable error \
                            to string: {}", e);
                        WsMessage::Text("Internal server error (Serializer)".to_string())
                    }
                };

                // Responds by writing an error to the socket
                if let Err(e) = sender.send(item).await {
                    error!("Failed to send error response to client: {}", e);
                }

                continue
            }
        };
        
        inbox.lock().await.push(opax_message.to_owned());

        let (apiresp, req) = match opax_message {
            OpaxMessage::APIRequest(r) => (
                response_listener(state.clone(), &r).await, r
            ),
            x => {
                error!("Client socket received unsupported message type: {:?}", x);
                resp.error = Some("Unsupported message type sent by the client".to_string());

                let item = match serde_json::to_string(&resp) {
                    Ok(s) => WsMessage::Text(s),
                    Err(e) => {
                        error!("Failed to serialize websocket bad message type error \
                            to string: {}", e);
                        WsMessage::Text("Internal server error (Serializer)".to_string())
                    }
                };

                if let Err(e) = sender.send(item).await {
                    error!("Failed to send error response to client: {}", e);
                }

                continue;
            },
        };

        // Update response instance and write it as a message to sender
        resp.request = Some(req);
        resp.response = apiresp;

        let item = match serde_json::to_string(&resp) {
            Ok(s) => WsMessage::Text(s),
            Err(e) => {
                error!("Failed to serialize websocket response to string: {}", e);
                WsMessage::Text("Internal server error (Serializer)".to_string())
            }
        };

        if let Err(e) = sender.send(item).await {
            error!("Failed to send response to client: {}", e);
        }
    }
}

#[derive(Serialize)]
enum APIRespFormat {
    Single(APIResp),
    Multiple(Vec<APIResp>)
}

impl APIRespFormat {
    fn to_error<A, T>(key: A, error: T) -> Json<Self>
    where A: Into<String>, T: Into<String> {
        Json(APIRespFormat::Single(APIResp {
            key: key.into(), exists: None, value: None, error: Some(error.into()),
            overwritten_value: None
        }))
    }
    fn restricted_key_error<T>(key: T) -> Option<Json<Self>> where T: AsRef<str> {
        if OPAX_RESERVED_KEYS.contains(&key.as_ref().as_bytes()) {
            return Some(Self::to_error(key.as_ref(), "Restricted key, access forbidden"))
        }

        None
    }
}

#[derive(Serialize)]
struct APIResp {
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exists: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overwritten_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl APIResp {
    fn new<T>(key: T) -> Self where T: Into<String> {
        Self { key: key.into(), exists: None, value: None, overwritten_value: None, error: None }
    }
    fn from_opax(resp: &OpaxResponse) -> Self {
        match resp.to_owned() {
            OpaxResponse::Get(d) => {
                Self { key: d.key, exists: Some(d.value.is_some()), error: d.error, value: d.value, overwritten_value: d.overwritten_val }
            },
            OpaxResponse::Put(d) => {
                Self { key: d.key, exists: None, error: d.error, value: d.value, overwritten_value: d.overwritten_val }
            },
            OpaxResponse::Delete(d) => {
                Self { key: d.key, exists: Some(d.overwritten_val.is_some()), error: d.error, value: None, overwritten_value: d.overwritten_val }
            },
            OpaxResponse::Pong((p, r)) => {
                let mut resp = Self::new(format!("{}", p));
                resp.value = Some(r);
                resp
            },
            OpaxResponse::Blocked(r) => {
                let mut resp = match r {
                    OpaxRequest::Put(e) => Self::new(&e.key),
                    OpaxRequest::Delete(k) => Self::new(&k),
                    x => panic!("Requests of type {:?} should not be blocked", x),
                };

                resp.error = Some("Action was blocked as too many cluster nodes have \
                    disconnected. Operation is permitted once more than half of the \
                    cluster nodes have returned".to_string());
                resp
            },
            x => panic!("Unable to build response from {:?}", x),
        }
    }
    fn to_error<A, T>(key: A, error: T) -> Self where A: Into<String>, T: Into<String> {
        Self {
            key: key.into(), exists: None, value: None, error: Some(error.into()),
            overwritten_value: None
        }
    }
}

async fn read_handler(Path(key): Path<String>, state: State<HttpServer>) -> Json<APIRespFormat> {
    if let Some(e) = APIRespFormat::restricted_key_error(&key) {
        return e
    }

    let req = OpaxRequest::Get(key.to_owned());
    let message = OpaxMessage::APIRequest(req.to_owned());

    debug!("GET request sent, wait for response: {:?}", message);
    state.inbox.lock().await.push(message);

    Json(APIRespFormat::Single(responder(state, req, key).await))
}

async fn write_handler(state: State<HttpServer>, Json(entry): Json<OpaxEntry>)
-> Json<APIRespFormat> {
    let key = entry.key.to_owned();
    
    if let Some(e) = APIRespFormat::restricted_key_error(&key) {
        return e
    }

    let req = OpaxRequest::Put(entry);
    let message = OpaxMessage::APIRequest(req.to_owned());

    debug!("PUT request sent, wait for response: {:?}", message);
    state.inbox.lock().await.push(message);
    
    Json(APIRespFormat::Single(responder(state, req, key).await))
}

async fn destroy_handler(Path(key): Path<String>, state: State<HttpServer>)
-> Json<APIRespFormat> {
    if let Some(e) = APIRespFormat::restricted_key_error(&key) {
        return e
    }

    let req = OpaxRequest::Delete(key.to_owned());
    let message = OpaxMessage::APIRequest(req.to_owned());
    
    debug!("Delete request sent, wait for response: {:?}", message);
    state.inbox.lock().await.push(message);

    Json(APIRespFormat::Single(responder(state, req, key).await))
}

async fn multi_read_handler(state: State<HttpServer>, Json(keys): Json<Vec<OpaxEntryKey>>)
-> Json<APIRespFormat> {
    let mut responses = vec![];
    let list = keys.clone().join(", ");

    debug!("Sending multiple GET requests for keys [{}]", list);

    for k in keys {
        if let Some(e) = APIRespFormat::restricted_key_error(&k) {
            return e
        }

        let req = OpaxRequest::Get(k.to_owned());
        let message = OpaxMessage::APIRequest(req.to_owned());

        state.inbox.lock().await.push(message);
        responses.push(responder(state.to_owned(), req, k).await);
    }

    Json(APIRespFormat::Multiple(responses))
}

async fn multi_write_handler(state: State<HttpServer>, Json(entries): Json<Vec<OpaxEntry>>)
-> Json<APIRespFormat> {
    let mut responses = vec![];
    let list = entries.iter()
        .map(|x|x.key.to_owned())
        .collect::<Vec<String>>()
        .join(", ");
    
    debug!("Sending multiple PUT requests for keys [{}]", list);

    for entry in entries {
        let key = entry.key.to_owned();

        if let Some(e) = APIRespFormat::restricted_key_error(&key) {
            return e
        }

        let req = OpaxRequest::Put(entry);
        let message = OpaxMessage::APIRequest(req.to_owned());

        state.inbox.lock().await.push(message);
        responses.push(responder(state.to_owned(), req, key).await);
    }

    Json(APIRespFormat::Multiple(responses))
}

async fn multi_destroy_handler(state: State<HttpServer>, Json(keys): Json<Vec<OpaxEntryKey>>)
-> Json<APIRespFormat> {
    let mut responses = vec![];
    let list = keys.clone().join(", ");

    debug!("Sending multiple DELETE requests for keys [{}]", list);

    for k in keys {
        if let Some(e) = APIRespFormat::restricted_key_error(&k) {
            return e
        }

        let req = OpaxRequest::Delete(k.to_owned());
        let message = OpaxMessage::APIRequest(req.to_owned());

        state.inbox.lock().await.push(message);
        responses.push(responder(state.to_owned(), req, k).await);
    }

    Json(APIRespFormat::Multiple(responses))
}

async fn ping_handler(state: State<HttpServer>) -> Json<APIRespFormat> {
    let mut responses = vec![];
    let list = state.peers.iter()
        .map(|(p, a)|format!("{}: {}", p, a.addr))
        .collect::<Vec<String>>()
        .join(", ");

    debug!("Sending multiple ping (GET) requests to peers [{}]", list);

    for p in state.peers.keys() {
        let req = OpaxRequest::Ping(*p);
        let message = OpaxMessage::APIRequest(req.to_owned());
        let key = format!("Ping peer node {}", p);
        
        state.inbox.lock().await.push(message);
        responses.push(responder(state.to_owned(), req, key).await);
    }

    Json(APIRespFormat::Multiple(responses))
}

async fn responder(state: State<HttpServer>, req: OpaxRequest, key: String) -> APIResp {
    match response_listener(state, &req).await {
        Some(r) => APIResp::from_opax(&r),
        None => APIResp::to_error(key, "Request timed out"),
    }
}

async fn response_listener(state: State<HttpServer>, req: &OpaxRequest) -> Option<OpaxResponse> {
    let interval = 50 as u64;
    let mut timeout = 3000 as u64;

    while timeout > 0 {
        let cache = state.responses.lock().await;

        if let Some(q) = cache.iter().find(|q|q.request.eq(&req)) {
            return Some(q.response.to_owned())
        }
        
        timeout = timeout - interval;

        if [500, 1500, 2000, 2500].contains(&timeout) {
            debug!("Waiting for response: {:?}...", req);
        }

        sleep(Duration::from_millis(50)).await;
    }
    
    None
}
