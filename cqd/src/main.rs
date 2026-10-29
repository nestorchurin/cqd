use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

use axum::{
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use chrono::Utc;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(RustEmbed)]
#[folder = "static/"]
struct Asset;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum OrderStatus {
    Registered,
    TechCardPending,
    Accepted,
    Mixing,
    Paused,
    Ready,
    Delivered,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderEvent {
    timestamp: i64,
    event_type: String,
    actor: String,
    details: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    id: String,
    order_number: String,
    client_name: String,
    material: String,
    color_code: String,
    base: Option<String>,
    total_buckets: u32,
    bucket_weight: String,
    buckets_completed: u32,
    status: OrderStatus,
    delay_reason: Option<String>,
    token: String,
    created_at: i64,
    accepted_at: Option<i64>,
    mixing_started_at: Option<i64>,
    mixing_duration_secs: Option<u64>,
    ready_at: Option<i64>,
    delivered_at: Option<i64>,
    events: Vec<OrderEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PushKeys {
    p256dh: String,
    auth: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PushSubscription {
    endpoint: String,
    keys: PushKeys,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CreateOrderRequest {
    order_number: String,
    client_name: String,
    material: String,
    total_buckets: u32,
    bucket_weight: String,
    color_code: String,
    base: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StartMixingRequest {
    duration_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PauseRequest {
    reason: String,
}

struct AppState {
    orders: HashMap<String, Order>,
    token_to_id: HashMap<String, String>,
    push_subs: HashMap<String, Vec<PushSubscription>>,
    vapid_public_key: String,
    vapid_private_key: String,
}

type SharedState = Arc<RwLock<AppState>>;

fn get_embedded_file(path: &str, content_type: &str) -> Response {
    match Asset::get(path) {
        Some(content) => {
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, content_type.parse().unwrap());
            (StatusCode::OK, headers, content.data).into_response()
        }
        None => (StatusCode::NOT_FOUND, "File Not Found").into_response(),
    }
}

// ==========================================
// PUBLIC HANDLERS (PORT 8080)
// ==========================================

async fn public_queue_page() -> Response {
    get_embedded_file("public_queue.html", "text/html; charset=utf-8")
}

#[derive(Serialize)]
struct PublicOrderSummary {
    order_number: String,
    client_name: String,
    material: String,
    color_code: String,
    base: Option<String>,
    total_buckets: u32,
    bucket_weight: String,
    buckets_completed: u32,
    status: OrderStatus,
    token: String,
    created_at: i64,
}

async fn public_get_queue(State(state): State<SharedState>) -> Json<Vec<PublicOrderSummary>> {
    let lock = state.read().await;
    let mut list: Vec<PublicOrderSummary> = lock
        .orders
        .values()
        .filter(|o| o.status != OrderStatus::Delivered)
        .map(|o| PublicOrderSummary {
            order_number: o.order_number.clone(),
            client_name: o.client_name.clone(),
            material: o.material.clone(),
            color_code: o.color_code.clone(),
            base: o.base.clone(),
            total_buckets: o.total_buckets,
            bucket_weight: o.bucket_weight.clone(),
            buckets_completed: o.buckets_completed,
            status: o.status.clone(),
            token: o.token.clone(),
            created_at: o.created_at,
        })
        .collect();
    list.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    Json(list)
}

async fn public_tracker_page(Path(_token): Path<String>) -> Response {
    get_embedded_file("customer.html", "text/html; charset=utf-8")
}

async fn public_service_worker() -> Response {
    let mut resp = get_embedded_file("sw.js", "application/javascript; charset=utf-8");
    resp.headers_mut()
        .insert("Service-Worker-Allowed", "/".parse().unwrap());
    resp
}

async fn public_manifest() -> Response {
    get_embedded_file("manifest.json", "application/manifest+json")
}

async fn public_get_order(
    Path(token): Path<String>,
    State(state): State<SharedState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let lock = state.read().await;
    let order_id = lock.token_to_id.get(&token).ok_or(StatusCode::NOT_FOUND)?;
    let order = lock.orders.get(order_id).ok_or(StatusCode::NOT_FOUND)?;

    let mut val = serde_json::to_value(order).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if let Some(obj) = val.as_object_mut() {
        obj.insert(
            "vapid_public_key".to_string(),
            serde_json::Value::String(lock.vapid_public_key.clone()),
        );
    }
    Ok(Json(val))
}

async fn public_subscribe_push(
    Path(token): Path<String>,
    State(state): State<SharedState>,
    Json(sub): Json<PushSubscription>,
) -> Result<StatusCode, StatusCode> {
    let mut lock = state.write().await;
    let order_id = lock
        .token_to_id
        .get(&token)
        .ok_or(StatusCode::NOT_FOUND)?
        .clone();

    lock.push_subs.entry(order_id).or_default().push(sub);
    println!("[WebPush] New subscription registered for token {}", token);
    Ok(StatusCode::OK)
}

// Rickroll Decoy Handler: Direct 301 redirect with autoplay
async fn rickroll_decoy_handler() -> Response {
    (
        StatusCode::MOVED_PERMANENTLY,
        [(
            header::LOCATION,
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ&autoplay=1",
        )],
        "Moved Permanently",
    )
        .into_response()
}

// ==========================================
// INTERNAL LOCAL HANDLERS (PORT 8081)
// ==========================================

async fn local_index() -> Response {
    (
        StatusCode::FOUND,
        [(header::LOCATION, "/terminal")],
        "Redirecting to terminal...",
    )
        .into_response()
}

async fn local_terminal_page() -> Response {
    get_embedded_file("terminal.html", "text/html; charset=utf-8")
}

async fn local_dispatcher_page() -> Response {
    get_embedded_file("terminal.html", "text/html; charset=utf-8")
}

async fn local_list_orders(State(state): State<SharedState>) -> Json<Vec<Order>> {
    let lock = state.read().await;
    let mut list: Vec<Order> = lock.orders.values().cloned().collect();
    list.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    Json(list)
}

async fn local_create_order(
    State(state): State<SharedState>,
    Json(req): Json<CreateOrderRequest>,
) -> Json<Order> {
    let mut lock = state.write().await;
    let id = Uuid::new_v4().to_string();
    let token = Uuid::new_v4().simple().to_string();

    let now = Utc::now().timestamp_millis();
    let order = Order {
        id: id.clone(),
        order_number: req.order_number.clone(),
        client_name: req.client_name,
        material: req.material,
        color_code: req.color_code,
        base: req.base,
        total_buckets: req.total_buckets,
        bucket_weight: req.bucket_weight,
        buckets_completed: 0,
        status: OrderStatus::TechCardPending,
        delay_reason: None,
        token: token.clone(),
        created_at: now,
        accepted_at: None,
        mixing_started_at: None,
        mixing_duration_secs: None,
        ready_at: None,
        delivered_at: None,
        events: vec![OrderEvent {
            timestamp: now,
            event_type: "CreatedInOffice".to_string(),
            actor: "Офіс".to_string(),
            details: Some(format!("Накладна {}", req.order_number)),
        }],
    };

    lock.token_to_id.insert(token, id.clone());
    lock.orders.insert(id, order.clone());
    println!("[Order] Created: #{} ({})", order.order_number, order.id);
    Json(order)
}

async fn local_accept_order(
    Path(id): Path<String>,
    State(state): State<SharedState>,
) -> Result<StatusCode, StatusCode> {
    let mut lock = state.write().await;
    let order = lock.orders.get_mut(&id).ok_or(StatusCode::NOT_FOUND)?;
    let now = Utc::now().timestamp_millis();

    order.status = OrderStatus::Accepted;
    order.accepted_at = Some(now);
    order.events.push(OrderEvent {
        timestamp: now,
        event_type: "AcceptedByColorist".to_string(),
        actor: "Колорист".to_string(),
        details: Some("Техкарту отримано, замовлення прийнято в роботу".to_string()),
    });

    println!("[Order] Accepted by operator: {}", order.order_number);
    Ok(StatusCode::OK)
}

async fn local_start_mixing(
    Path(id): Path<String>,
    State(state): State<SharedState>,
    Json(req): Json<StartMixingRequest>,
) -> Result<StatusCode, StatusCode> {
    let mut lock = state.write().await;
    let order = lock.orders.get_mut(&id).ok_or(StatusCode::NOT_FOUND)?;
    let now = Utc::now().timestamp_millis();

    order.status = OrderStatus::Mixing;
    order.mixing_started_at = Some(now);
    order.mixing_duration_secs = Some(req.duration_secs);
    order.events.push(OrderEvent {
        timestamp: now,
        event_type: "MixingStarted".to_string(),
        actor: "Гіроміксер".to_string(),
        details: Some(format!(
            "Відро {}/{} (тривалість {}с)",
            order.buckets_completed + 1,
            order.total_buckets,
            req.duration_secs
        )),
    });

    Ok(StatusCode::OK)
}

async fn local_finish_bucket(
    Path(id): Path<String>,
    State(state): State<SharedState>,
) -> Result<StatusCode, StatusCode> {
    let mut lock = state.write().await;
    let order = lock.orders.get_mut(&id).ok_or(StatusCode::NOT_FOUND)?;
    let now = Utc::now().timestamp_millis();

    order.buckets_completed += 1;
    order.events.push(OrderEvent {
        timestamp: now,
        event_type: "BucketFinished".to_string(),
        actor: "Гіроміксер".to_string(),
        details: Some(format!(
            "Відро {}/{} завершено",
            order.buckets_completed, order.total_buckets
        )),
    });

    if order.buckets_completed >= order.total_buckets {
        order.status = OrderStatus::Accepted; // ready to be marked ready
    }

    Ok(StatusCode::OK)
}

async fn local_pause_order(
    Path(id): Path<String>,
    State(state): State<SharedState>,
    Json(req): Json<PauseRequest>,
) -> Result<StatusCode, StatusCode> {
    let mut lock = state.write().await;
    let order = lock.orders.get_mut(&id).ok_or(StatusCode::NOT_FOUND)?;
    let now = Utc::now().timestamp_millis();

    order.status = OrderStatus::Paused;
    order.delay_reason = Some(req.reason.clone());
    order.events.push(OrderEvent {
        timestamp: now,
        event_type: "DelayPause".to_string(),
        actor: "Колорист".to_string(),
        details: Some(req.reason),
    });

    println!("[Order] Delay recorded: {} - {:?}", order.order_number, order.delay_reason);
    Ok(StatusCode::OK)
}

async fn local_resume_order(
    Path(id): Path<String>,
    State(state): State<SharedState>,
) -> Result<StatusCode, StatusCode> {
    let mut lock = state.write().await;
    let order = lock.orders.get_mut(&id).ok_or(StatusCode::NOT_FOUND)?;
    let now = Utc::now().timestamp_millis();

    order.status = OrderStatus::Accepted;
    order.delay_reason = None;
    order.events.push(OrderEvent {
        timestamp: now,
        event_type: "DelayResumed".to_string(),
        actor: "Колорист".to_string(),
        details: Some("Роботу відновлено після затримки".to_string()),
    });

    Ok(StatusCode::OK)
}

async fn local_mark_ready(
    Path(id): Path<String>,
    State(state): State<SharedState>,
) -> Result<StatusCode, StatusCode> {
    let (subs, order_num, material, buckets) = {
        let mut lock = state.write().await;
        let order = lock.orders.get_mut(&id).ok_or(StatusCode::NOT_FOUND)?;
        let now = Utc::now().timestamp_millis();

        order.status = OrderStatus::Ready;
        order.ready_at = Some(now);
        order.events.push(OrderEvent {
            timestamp: now,
            event_type: "ReadyForPickup".to_string(),
            actor: "Колорист".to_string(),
            details: Some("Усі відра заколіровано, продукція на рампі".to_string()),
        });

        let order_num = order.order_number.clone();
        let material = order.material.clone();
        let buckets = order.total_buckets;
        let subs = lock.push_subs.get(&id).cloned().unwrap_or_default();
        (subs, order_num, material, buckets)
    };

    println!("[Order] READY FOR PICKUP: #{}", order_num);

    // Asynchronously dispatch Web Push notifications
    if !subs.is_empty() {
        let state_clone = Arc::clone(&state);
        tokio::spawn(async move {
            let (priv_key, pub_key) = {
                let lock = state_clone.read().await;
                (lock.vapid_private_key.clone(), lock.vapid_public_key.clone())
            };
            send_web_push_notifications(subs, order_num, material, buckets, priv_key, pub_key).await;
        });
    }

    Ok(StatusCode::OK)
}

async fn local_mark_delivered(
    Path(id): Path<String>,
    State(state): State<SharedState>,
) -> Result<StatusCode, StatusCode> {
    let mut lock = state.write().await;
    let order = lock.orders.get_mut(&id).ok_or(StatusCode::NOT_FOUND)?;
    let now = Utc::now().timestamp_millis();

    order.status = OrderStatus::Delivered;
    order.delivered_at = Some(now);
    order.events.push(OrderEvent {
        timestamp: now,
        event_type: "DeliveredToDriver".to_string(),
        actor: "Колорист".to_string(),
        details: Some("Партія видана водію, замовлення закрито в архів".to_string()),
    });

    println!("[Order] Delivered & Archived: #{}", order.order_number);
    Ok(StatusCode::OK)
}

async fn local_delete_order(
    Path(id): Path<String>,
    State(state): State<SharedState>,
) -> Result<StatusCode, StatusCode> {
    let mut lock = state.write().await;
    if let Some(order) = lock.orders.remove(&id) {
        lock.token_to_id.remove(&order.token);
        lock.push_subs.remove(&id);
        println!("[Order] DELETED order: #{} ({})", order.order_number, id);
        Ok(StatusCode::OK)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// Async Web Push Dispatcher
async fn send_web_push_notifications(
    subs: Vec<PushSubscription>,
    order_number: String,
    material: String,
    buckets: u32,
    private_key: String,
    _public_key: String,
) {
    use web_push::*;

    println!("[WebPush] Dispatching notifications to {} client(s)...", subs.len());

    let payload = serde_json::json!({
        "title": "Ceresit Колірувальний Цех",
        "body": format!("Ваше замовлення {} ({} відер {}) заколіровано та готове до завантаження на рампі! 🪣", order_number, buckets, material),
        "url": "/"
    }).to_string();

    for sub in subs {
        let sub_info = SubscriptionInfo::new(
            sub.endpoint.clone(),
            sub.keys.p256dh.clone(),
            sub.keys.auth.clone(),
        );

        let mut builder = WebPushMessageBuilder::new(&sub_info);
        builder.set_payload(ContentEncoding::Aes128Gcm, payload.as_bytes());

        if let Ok(sig_builder) = VapidSignatureBuilder::from_base64(&private_key, &sub_info) {
            if let Ok(sig) = sig_builder.build() {
                builder.set_vapid_signature(sig);
            }
        }

        if let Ok(msg) = builder.build() {
            let client = IsahcWebPushClient::new();
            match client {
                Ok(cli) => {
                    match cli.send(msg).await {
                        Ok(_) => println!("[WebPush] Successfully delivered push to: {}", sub.endpoint),
                        Err(e) => eprintln!("[WebPush] Delivery error: {:?}", e),
                    }
                }
                Err(e) => eprintln!("[WebPush] Client init error: {:?}", e),
            }
        }
    }
}

// ==========================================
// MAIN ENTRYPOINT
// ==========================================

#[tokio::main]
async fn main() {
    println!("==================================================");
    println!("      CERESIT QUEUE DAEMON (cqd) v1.0.0");
    println!("  Autonomous Production & Dispatch System");
    println!("==================================================");

    // Generate or use VAPID keys
    let vapid_public_key = std::env::var("VAPID_PUBLIC_KEY").unwrap_or_else(|_| {
        "BCg27wQn0kUjA6w031uL3oOvdT64g_lD4w82tF1sI0_eY2QoR2e_4o9M7w9K1yX2m4Q4U4y2k4u8u4_k2=".to_string()
    });
    let vapid_private_key = std::env::var("VAPID_PRIVATE_KEY").unwrap_or_else(|_| {
        "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg...".to_string()
    });

    let state: SharedState = Arc::new(RwLock::new(AppState {
        orders: HashMap::new(),
        token_to_id: HashMap::new(),
        push_subs: HashMap::new(),
        vapid_public_key,
        vapid_private_key,
    }));

    // ----------------------------------------------------
    // 1. PUBLIC ROUTER (Port 8080: Cloudflare Tunnel ONLY)
    // ----------------------------------------------------
    let public_app = Router::new()
        .route("/", get(public_queue_page))
        .route("/api/public/queue", get(public_get_queue))
        .route("/track/{token}", get(public_tracker_page))
        .route("/api/public/orders/{token}", get(public_get_order))
        .route("/api/public/orders/{token}/subscribe", post(public_subscribe_push))
        .route("/sw.js", get(public_service_worker))
        .route("/manifest.json", get(public_manifest))
        .route("/health", get(|| async { "OK" }))
        // RICKROLL DECOY: Catch-all for ANY internal/admin probing attempts!
        .fallback(rickroll_decoy_handler)
        .with_state(Arc::clone(&state));

    // ----------------------------------------------------
    // 2. INTERNAL LOCAL ROUTER (Port 8081: LAN ONLY)
    // ----------------------------------------------------
    let local_app = Router::new()
        .route("/", get(local_index))
        .route("/terminal", get(local_terminal_page))
        .route("/dispatcher", get(local_dispatcher_page))
        .route("/admin", get(local_dispatcher_page))
        .route("/api/admin/orders", get(local_list_orders).post(local_create_order))
        .route("/api/admin/orders/{id}", delete(local_delete_order))
        .route("/api/terminal/orders/{id}/delete", post(local_delete_order))
        .route("/api/terminal/orders/{id}/accept", post(local_accept_order))
        .route("/api/terminal/orders/{id}/start-mixing", post(local_start_mixing))
        .route("/api/terminal/orders/{id}/finish-bucket", post(local_finish_bucket))
        .route("/api/terminal/orders/{id}/pause", post(local_pause_order))
        .route("/api/terminal/orders/{id}/resume", post(local_resume_order))
        .route("/api/terminal/orders/{id}/ready", post(local_mark_ready))
        .route("/api/terminal/orders/{id}/delivered", post(local_mark_delivered))
        // Also allow local preview of track page
        .route("/track/{token}", get(public_tracker_page))
        .route("/api/public/orders/{token}", get(public_get_order))
        .route("/sw.js", get(public_service_worker))
        .route("/manifest.json", get(public_manifest))
        .with_state(Arc::clone(&state));

    // Start Public Listener on 0.0.0.0:8080
    let pub_addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    println!("[Listener 1] PUBLIC Tracker & Rickroll Decoy on http://{}", pub_addr);
    let pub_server = async move {
        let listener = tokio::net::TcpListener::bind(pub_addr).await.unwrap();
        axum::serve(listener, public_app).await.unwrap();
    };

    // Start Internal Local Listener on 0.0.0.0:8081
    let local_addr = SocketAddr::from(([0, 0, 0, 0], 8081));
    println!("[Listener 2] INTERNAL Workshop & Dispatcher on http://{}", local_addr);
    let local_server = async move {
        let listener = tokio::net::TcpListener::bind(local_addr).await.unwrap();
        axum::serve(listener, local_app).await.unwrap();
    };

    // Run both listeners concurrently
    tokio::select! {
        _ = pub_server => eprintln!("Public server exited"),
        _ = local_server => eprintln!("Local server exited"),
    }
}
