mod db;

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
pub enum OrderStatus {
    Registered,
    TechCardPending,
    Accepted,
    Mixing,
    Paused,
    Ready,
    Delivered,
}

impl OrderStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderStatus::Registered => "Registered",
            OrderStatus::TechCardPending => "TechCardPending",
            OrderStatus::Accepted => "Accepted",
            OrderStatus::Mixing => "Mixing",
            OrderStatus::Paused => "Paused",
            OrderStatus::Ready => "Ready",
            OrderStatus::Delivered => "Delivered",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "Registered" => OrderStatus::Registered,
            "Accepted" => OrderStatus::Accepted,
            "Mixing" => OrderStatus::Mixing,
            "Paused" => OrderStatus::Paused,
            "Ready" => OrderStatus::Ready,
            "Delivered" => OrderStatus::Delivered,
            _ => OrderStatus::TechCardPending,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderEvent {
    pub timestamp: i64,
    pub iso_time: String,
    pub event_type: String,
    pub actor: String,
    pub details: Option<String>,
    pub prev_hash: String,
    pub signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub order_number: String,
    pub client_name: String,
    pub material: String,
    pub collection: Option<String>,
    pub color_code: String,
    pub base: Option<String>,
    pub total_buckets: u32,
    pub bucket_weight: String,
    pub buckets_completed: u32,
    pub status: OrderStatus,
    pub delay_reason: Option<String>,
    pub token: String,
    pub created_at: i64,
    pub accepted_at: Option<i64>,
    pub mixing_started_at: Option<i64>,
    pub mixing_duration_secs: Option<u64>,
    pub ready_at: Option<i64>,
    pub delivered_at: Option<i64>,
    pub is_archived: bool,
    pub events: Vec<OrderEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushKeys {
    pub p256dh: String,
    pub auth: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushSubscription {
    pub endpoint: String,
    pub keys: PushKeys,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateOrderRequest {
    pub order_number: Option<String>,
    pub client_name: Option<String>,
    pub material: String,
    pub total_buckets: u32,
    pub bucket_weight: String,
    pub collection: Option<String>,
    pub color_code: Option<String>,
    pub base: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartMixingRequest {
    pub duration_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PauseRequest {
    pub reason: String,
}

pub struct AppState {
    pub db: std::sync::Mutex<rusqlite::Connection>,
    pub orders: HashMap<String, Order>,
    pub token_to_id: HashMap<String, String>,
    pub push_subs: HashMap<String, Vec<PushSubscription>>,
    pub vapid_public_key: String,
    pub vapid_private_key: String,
    pub signing_secret: Vec<u8>,
    pub last_event_hash: String,
}

impl AppState {
    pub fn record_event(
        &mut self,
        order_id: &str,
        event_type: &str,
        actor: &str,
        details: Option<&str>,
    ) -> Result<OrderEvent, StatusCode> {
        let &mut AppState {
            ref db,
            ref signing_secret,
            ref mut last_event_hash,
            ..
        } = self;
        let mut db_conn = db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        db::record_event(
            &mut db_conn,
            signing_secret,
            last_event_hash,
            order_id,
            event_type,
            actor,
            details,
        )
        .map_err(|e| {
            eprintln!("[SQLite Error] record_event failed: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
    }

    pub fn create_order(
        &mut self,
        order: Order,
        actor: &str,
        details: Option<&str>,
    ) -> Result<Order, StatusCode> {
        let &mut AppState {
            ref db,
            ref signing_secret,
            ref mut last_event_hash,
            ..
        } = self;
        let mut db_conn = db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        db::create_order_with_event(
            &mut db_conn,
            signing_secret,
            last_event_hash,
            order,
            actor,
            details,
        )
        .map_err(|e| {
            eprintln!("[SQLite Error] create_order_with_event failed: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
    }
}

pub type SharedState = Arc<RwLock<AppState>>;

fn get_embedded_file(path: &str, content_type: &str) -> Response {
    match Asset::get(path) {
        Some(content) => {
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, content_type.parse().unwrap());
            headers.insert(
                header::CACHE_CONTROL,
                "no-cache, no-store, must-revalidate".parse().unwrap(),
            );
            headers.insert(header::PRAGMA, "no-cache".parse().unwrap());
            headers.insert(header::EXPIRES, "0".parse().unwrap());
            (StatusCode::OK, headers, content.data).into_response()
        }
        None => (StatusCode::NOT_FOUND, "File Not Found").into_response(),
    }
}

// ==========================================
// PUBLIC HANDLERS (PORT 8080)
// ==========================================

#[derive(Debug, Clone, Serialize, Deserialize)]
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
        .filter(|o| !o.is_archived && o.status != OrderStatus::Delivered)
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

async fn public_queue_page() -> Response {
    get_embedded_file("public_queue.html", "text/html; charset=utf-8")
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
    let order = if let Some(order_id) = lock.token_to_id.get(&token) {
        lock.orders.get(order_id).cloned()
    } else {
        None
    };

    let order = match order {
        Some(o) => o,
        None => {
            let db_conn = lock.db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            match db::load_order_by_token(&db_conn, &token) {
                Ok(Some(o)) => o,
                _ => return Err(StatusCode::NOT_FOUND),
            }
        }
    };

    let mut val = serde_json::to_value(&order).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
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
    let order_id = if let Some(oid) = lock.token_to_id.get(&token) {
        oid.clone()
    } else {
        let db_conn = lock.db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        match db::load_order_by_token(&db_conn, &token) {
            Ok(Some(o)) => o.id,
            _ => return Err(StatusCode::NOT_FOUND),
        }
    };

    {
        let mut db_conn = lock.db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let _ = db::save_push_subscription(&mut db_conn, &order_id, &sub);
    }

    lock.push_subs.entry(order_id.clone()).or_default().push(sub);
    lock.token_to_id.insert(token, order_id);
    Ok(StatusCode::OK)
}

async fn rickroll_decoy_handler() -> Response {
    (
        StatusCode::MOVED_PERMANENTLY,
        [(header::LOCATION, "https://www.youtube.com/watch?v=dQw4w9WgXcQ&autoplay=1")],
        "301 Redirecting...",
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
    let mut list: Vec<Order> = lock.orders.values().filter(|o| !o.is_archived).cloned().collect();
    list.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    Json(list)
}

async fn local_get_archive(State(state): State<SharedState>) -> Result<Json<Vec<Order>>, StatusCode> {
    let lock = state.read().await;
    let db_conn = lock.db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let archived = db::load_archived_orders(&db_conn, 100).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(archived))
}

fn get_next_order_number(conn: &rusqlite::Connection) -> String {
    let mut max_num = 1040;
    if let Ok(mut stmt) = conn.prepare("SELECT order_number FROM orders") {
        if let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(0)) {
            for r in rows.flatten() {
                let clean = r.trim().trim_start_matches('#');
                if let Ok(n) = clean.parse::<u64>() {
                    if n > max_num {
                        max_num = n;
                    }
                }
            }
        }
    }
    format!("#{}", max_num + 1)
}

async fn local_next_order_number(
    State(state): State<SharedState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let lock = state.read().await;
    let db_conn = lock.db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let next_num = get_next_order_number(&db_conn);
    Ok(Json(serde_json::json!({ "next_order_number": next_num })))
}

async fn local_scan_recipe(
    State(state): State<SharedState>,
    body: axum::body::Bytes,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let temp_img_path = "/tmp/cqd_scan_input.jpg";
    if let Err(e) = std::fs::write(temp_img_path, &body) {
        eprintln!("[Scan Error] Failed to write scan input: {:?}", e);
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    let ocr_script = "/usr/local/bin/cqd-ocr.py";
    let output = std::process::Command::new(ocr_script)
        .arg(temp_img_path)
        .output()
        .map_err(|e| {
            eprintln!("[Scan Error] Failed to execute ocr script: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|_| {
        serde_json::json!({
            "client_name": "",
            "material": "Ceresit CT 174 (1.5mm) Камінцева",
            "color_code": "",
            "base": "База A",
            "bucket_weight": "25 кг",
            "total_buckets": 1
        })
    });

    let lock = state.read().await;
    let next_num = if let Ok(db_conn) = lock.db.lock() {
        get_next_order_number(&db_conn)
    } else {
        "#1044".to_string()
    };

    Ok(Json(serde_json::json!({
        "success": true,
        "order_number": next_num,
        "data": parsed
    })))
}

async fn local_create_order(
    State(state): State<SharedState>,
    Json(req): Json<CreateOrderRequest>,
) -> Result<Json<Order>, StatusCode> {
    let mut lock = state.write().await;
    let id = Uuid::new_v4().to_string();
    let token = Uuid::new_v4().simple().to_string();
    let now = Utc::now().timestamp_millis();

    let final_order_number = match req.order_number {
        Some(ref s) if !s.trim().is_empty() => {
            let t = s.trim();
            if t.starts_with('#') {
                t.to_string()
            } else {
                format!("#{}", t)
            }
        }
        _ => {
            let db_conn = lock.db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            get_next_order_number(&db_conn)
        }
    };

    let final_client_name = match req.client_name {
        Some(s) if !s.trim().is_empty() => s.trim().to_string(),
        _ => "Клієнт (Самовивіз)".to_string(),
    };

    let final_color_code = match req.color_code {
        Some(s) if !s.trim().is_empty() => s.trim().to_string(),
        _ => "Стандарт".to_string(),
    };

    let final_collection = req.collection.and_then(|s| {
        let t = s.trim().to_string();
        if t.is_empty() { None } else { Some(t) }
    });

    let order = Order {
        id: id.clone(),
        order_number: final_order_number.clone(),
        client_name: final_client_name,
        material: req.material,
        collection: final_collection,
        color_code: final_color_code,
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
        is_archived: false,
        events: vec![],
    };

    let persisted_order = lock.create_order(
        order,
        "Офіс",
        Some(&format!("Накладна {}", final_order_number)),
    )?;

    lock.token_to_id.insert(token, id.clone());
    lock.orders.insert(id, persisted_order.clone());
    println!("[Order] Created & Persisted to SQLite: #{} ({})", persisted_order.order_number, persisted_order.id);
    Ok(Json(persisted_order))
}

async fn local_accept_order(
    Path(id): Path<String>,
    State(state): State<SharedState>,
) -> Result<StatusCode, StatusCode> {
    let mut lock = state.write().await;
    if !lock.orders.contains_key(&id) {
        return Err(StatusCode::NOT_FOUND);
    }
    let now = Utc::now().timestamp_millis();

    let event = lock.record_event(
        &id,
        "AcceptedByColorist",
        "Колорист",
        Some("Техкарту отримано, замовлення прийнято в роботу"),
    )?;

    {
        let mut db_conn = lock.db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        db::update_order_status(
            &mut db_conn,
            &id,
            &OrderStatus::Accepted,
            Some(now),
            None,
            None,
            false,
        )
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    let order = lock.orders.get_mut(&id).unwrap();
    order.status = OrderStatus::Accepted;
    order.accepted_at = Some(now);
    order.events.push(event);

    println!("[Order] Accepted by operator: {}", order.order_number);
    Ok(StatusCode::OK)
}

async fn local_start_mixing(
    Path(id): Path<String>,
    State(state): State<SharedState>,
    Json(req): Json<StartMixingRequest>,
) -> Result<StatusCode, StatusCode> {
    let mut lock = state.write().await;
    let (buckets_completed, total_buckets) = {
        let order = lock.orders.get(&id).ok_or(StatusCode::NOT_FOUND)?;
        (order.buckets_completed, order.total_buckets)
    };
    let now = Utc::now().timestamp_millis();
    let details = format!(
        "Відро {}/{} (тривалість {}с)",
        buckets_completed + 1,
        total_buckets,
        req.duration_secs
    );

    let event = lock.record_event(&id, "MixingStarted", "Гіроміксер", Some(&details))?;

    {
        let mut db_conn = lock.db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        db::update_order_status(
            &mut db_conn,
            &id,
            &OrderStatus::Mixing,
            None,
            None,
            None,
            false,
        )
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    let order = lock.orders.get_mut(&id).unwrap();
    order.status = OrderStatus::Mixing;
    order.mixing_started_at = Some(now);
    order.mixing_duration_secs = Some(req.duration_secs);
    order.events.push(event);

    Ok(StatusCode::OK)
}

async fn local_finish_bucket(
    Path(id): Path<String>,
    State(state): State<SharedState>,
) -> Result<StatusCode, StatusCode> {
    let mut lock = state.write().await;
    let (new_completed, total_buckets) = {
        let order = lock.orders.get(&id).ok_or(StatusCode::NOT_FOUND)?;
        (order.buckets_completed + 1, order.total_buckets)
    };

    let new_status = if new_completed >= total_buckets {
        OrderStatus::Accepted
    } else {
        OrderStatus::Mixing
    };

    let details = format!("Відро {}/{} завершено", new_completed, total_buckets);
    let event = lock.record_event(&id, "BucketFinished", "Гіроміксер", Some(&details))?;

    {
        let mut db_conn = lock.db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        db::update_order_buckets(&mut db_conn, &id, new_completed, &new_status)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    let order = lock.orders.get_mut(&id).unwrap();
    order.buckets_completed = new_completed;
    order.status = new_status;
    order.events.push(event);

    Ok(StatusCode::OK)
}

async fn local_decrement_bucket(
    Path(id): Path<String>,
    State(state): State<SharedState>,
) -> Result<StatusCode, StatusCode> {
    let mut lock = state.write().await;
    let (new_completed, total_buckets) = {
        let order = lock.orders.get(&id).ok_or(StatusCode::NOT_FOUND)?;
        if order.buckets_completed == 0 {
            return Ok(StatusCode::OK);
        }
        (order.buckets_completed - 1, order.total_buckets)
    };

    let new_status = if new_completed == 0 {
        OrderStatus::Accepted
    } else {
        OrderStatus::Mixing
    };

    let details = format!("Відкат відра: {}/{}", new_completed, total_buckets);
    let event = lock.record_event(&id, "BucketRollback", "Цех", Some(&details))?;

    {
        let mut db_conn = lock.db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        db::update_order_buckets(&mut db_conn, &id, new_completed, &new_status)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    let order = lock.orders.get_mut(&id).unwrap();
    order.buckets_completed = new_completed;
    order.status = new_status;
    order.events.push(event);

    Ok(StatusCode::OK)
}

async fn local_pause_order(
    Path(id): Path<String>,
    State(state): State<SharedState>,
    Json(req): Json<PauseRequest>,
) -> Result<StatusCode, StatusCode> {
    let mut lock = state.write().await;
    if !lock.orders.contains_key(&id) {
        return Err(StatusCode::NOT_FOUND);
    }

    let event = lock.record_event(&id, "DelayPause", "Колорист", Some(&req.reason))?;

    {
        let mut db_conn = lock.db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        db::update_order_pause(&mut db_conn, &id, &OrderStatus::Paused, Some(&req.reason))
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    let order = lock.orders.get_mut(&id).unwrap();
    order.status = OrderStatus::Paused;
    order.delay_reason = Some(req.reason.clone());
    order.events.push(event);

    println!("[Order] Delay recorded: {} - {:?}", order.order_number, order.delay_reason);
    Ok(StatusCode::OK)
}

async fn local_resume_order(
    Path(id): Path<String>,
    State(state): State<SharedState>,
) -> Result<StatusCode, StatusCode> {
    let mut lock = state.write().await;
    if !lock.orders.contains_key(&id) {
        return Err(StatusCode::NOT_FOUND);
    }

    let event = lock.record_event(&id, "DelayResumed", "Колорист", Some("Роботу відновлено після затримки"))?;

    {
        let mut db_conn = lock.db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        db::update_order_pause(&mut db_conn, &id, &OrderStatus::Accepted, None)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    let order = lock.orders.get_mut(&id).unwrap();
    order.status = OrderStatus::Accepted;
    order.delay_reason = None;
    order.events.push(event);

    Ok(StatusCode::OK)
}

async fn local_mark_ready(
    Path(id): Path<String>,
    State(state): State<SharedState>,
) -> Result<StatusCode, StatusCode> {
    let (subs, order_num, material, buckets) = {
        let mut lock = state.write().await;
        let (order_num, material, buckets) = {
            let order = lock.orders.get(&id).ok_or(StatusCode::NOT_FOUND)?;
            (order.order_number.clone(), order.material.clone(), order.total_buckets)
        };
        let now = Utc::now().timestamp_millis();

        let event = lock.record_event(
            &id,
            "ReadyForPickup",
            "Колорист",
            Some("Усі відра заколіровано, продукція на рампі"),
        )?;

        {
            let mut db_conn = lock.db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            db::update_order_status(
                &mut db_conn,
                &id,
                &OrderStatus::Ready,
                None,
                Some(now),
                None,
                false,
            )
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        }

        let subs = lock.push_subs.get(&id).cloned().unwrap_or_default();
        let order = lock.orders.get_mut(&id).unwrap();
        order.status = OrderStatus::Ready;
        order.ready_at = Some(now);
        order.events.push(event);

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
    let order_num = {
        let order = lock.orders.get(&id).ok_or(StatusCode::NOT_FOUND)?;
        order.order_number.clone()
    };
    let now = Utc::now().timestamp_millis();

    let event = lock.record_event(
        &id,
        "DeliveredToClient",
        "Колорист",
        Some("Партія видана клієнту, замовлення закрито в архів"),
    )?;

    {
        let mut db_conn = lock.db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        db::update_order_status(
            &mut db_conn,
            &id,
            &OrderStatus::Delivered,
            None,
            None,
            Some(now),
            true, // is_archived = true
        )
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    let order = lock.orders.get_mut(&id).unwrap();
    order.status = OrderStatus::Delivered;
    order.delivered_at = Some(now);
    order.is_archived = true;
    order.events.push(event);

    println!("[Order] Delivered & Archived to SQLite: #{}", order_num);
    Ok(StatusCode::OK)
}

async fn local_delete_order(
    Path(id): Path<String>,
    State(state): State<SharedState>,
) -> Result<StatusCode, StatusCode> {
    let mut lock = state.write().await;
    let db_res = {
        let mut db_conn = lock.db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        db::delete_order(&mut db_conn, &id)
    };
    if let Err(e) = db_res {
        eprintln!("[SQLite Error] Failed to delete order {}: {:?}", id, e);
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }
    if let Some(order) = lock.orders.remove(&id) {
        lock.token_to_id.remove(&order.token);
        lock.push_subs.remove(&id);
        println!("[Order] DELETED active order from SQLite and RAM: #{} ({})", order.order_number, id);
    } else {
        println!("[Order] DELETED archived/persisted order from SQLite: {}", id);
    }
    Ok(StatusCode::OK)
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

    // 1. Initialize SQLite Database
    let db_path = std::env::var("DATABASE_PATH").unwrap_or_else(|_| {
        if std::path::Path::new("/var/lib/cqd").exists() {
            "/var/lib/cqd/cqd.db".to_string()
        } else {
            "./cqd.db".to_string()
        }
    });

    let (conn, signing_secret, last_event_hash) = db::init_db(&db_path)
        .expect("Failed to initialize SQLite persistent database");

    // Load active orders and push subscriptions from SQLite
    let active_orders = db::load_active_orders(&conn).unwrap_or_default();
    let mut token_to_id = HashMap::new();
    for o in active_orders.values() {
        token_to_id.insert(o.token.clone(), o.id.clone());
    }
    let push_subs = db::load_push_subscriptions(&conn).unwrap_or_default();
    println!(
        "[SQLite] Loaded {} active order(s) and {} push subscription(s) from persistent storage.",
        active_orders.len(),
        push_subs.len()
    );

    // Generate or use VAPID keys (ECDSA P-256 standard for Web Push)
    let vapid_public_key = std::env::var("VAPID_PUBLIC_KEY").unwrap_or_else(|_| {
        "BMjQIp55pdbU8pfCBKyXcZjlmER_mXt5LqNrN1hrXbdBS5EnhIbMu3Au-RV53iIpztzNXkGI56BFB1udQ8Bq_H4".to_string()
    });
    let vapid_private_key = std::env::var("VAPID_PRIVATE_KEY").unwrap_or_else(|_| {
        "IQ9Ur0ykXoHS9gzfYX0aBjy9lvdrjx_PFUXmie9YRcY".to_string()
    });

    let state: SharedState = Arc::new(RwLock::new(AppState {
        db: std::sync::Mutex::new(conn),
        orders: active_orders,
        token_to_id,
        push_subs,
        vapid_public_key,
        vapid_private_key,
        signing_secret,
        last_event_hash,
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
        .route("/api/admin/archive", get(local_get_archive))
        .route("/api/admin/orders/{id}", delete(local_delete_order))
        .route("/api/terminal/orders/{id}/delete", post(local_delete_order))
        .route("/api/terminal/orders/{id}/accept", post(local_accept_order))
        .route("/api/terminal/orders/{id}/start-mixing", post(local_start_mixing))
        .route("/api/terminal/orders/{id}/finish-bucket", post(local_finish_bucket))
        .route("/api/terminal/orders/{id}/decrement-bucket", post(local_decrement_bucket))
        .route("/api/terminal/orders/{id}/pause", post(local_pause_order))
        .route("/api/terminal/orders/{id}/resume", post(local_resume_order))
        .route("/api/terminal/orders/{id}/ready", post(local_mark_ready))
        .route("/api/terminal/orders/{id}/delivered", post(local_mark_delivered))
        .route("/api/terminal/next-order-number", get(local_next_order_number))
        .route("/api/terminal/scan-recipe", post(local_scan_recipe))
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
