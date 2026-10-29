use std::collections::HashMap;
use chrono::Utc;
use hmac::{Hmac, Mac};
use rusqlite::{params, Connection, Result};
use sha2::{Digest, Sha256};

use crate::{Order, OrderEvent, OrderStatus, PushKeys, PushSubscription};

type HmacSha256 = Hmac<Sha256>;

pub fn generate_event_signature(
    secret: &[u8],
    prev_hash: &str,
    timestamp: i64,
    order_id: &str,
    event_type: &str,
    details: Option<&str>,
) -> (String, String) {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    let det = details.unwrap_or("");
    let payload = format!("{}:{}:{}:{}:{}", prev_hash, timestamp, order_id, event_type, det);
    mac.update(payload.as_bytes());
    let sig_bytes = mac.finalize().into_bytes();
    let signature = hex::encode(sig_bytes);

    let mut hasher = Sha256::new();
    hasher.update(signature.as_bytes());
    let new_hash = hex::encode(hasher.finalize());

    (signature, new_hash)
}

pub fn init_db(db_path: &str) -> Result<(Connection, Vec<u8>, String)> {
    println!("[SQLite] Connecting to database at: {}", db_path);
    let conn = Connection::open(db_path)?;

    // Optimize SQLite for high performance
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA foreign_keys = ON;
         PRAGMA busy_timeout = 5000;"
    )?;

    // Create tables
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY,
            order_number TEXT NOT NULL,
            client_name TEXT NOT NULL,
            material TEXT NOT NULL,
            collection TEXT,
            color_code TEXT NOT NULL,
            base TEXT,
            total_buckets INTEGER NOT NULL,
            bucket_weight TEXT NOT NULL,
            buckets_completed INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            delay_reason TEXT,
            token TEXT NOT NULL UNIQUE,
            created_at INTEGER NOT NULL,
            accepted_at INTEGER,
            mixing_started_at INTEGER,
            mixing_duration_secs INTEGER,
            ready_at INTEGER,
            delivered_at INTEGER,
            is_archived INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS order_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            iso_time TEXT NOT NULL,
            event_type TEXT NOT NULL,
            actor TEXT NOT NULL,
            details TEXT,
            prev_hash TEXT NOT NULL,
            signature TEXT NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS push_subscriptions (
            order_id TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            p256dh TEXT NOT NULL,
            auth TEXT NOT NULL,
            PRIMARY KEY (order_id, endpoint),
            FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS system_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );"
    )?;

    // Safe migration: add collection column if table already exists
    let _ = conn.execute("ALTER TABLE orders ADD COLUMN collection TEXT", []);

    // 1. Get or generate signing secret
    let secret_res: Result<String> = conn.query_row(
        "SELECT value FROM system_meta WHERE key = 'signing_secret'",
        [],
        |row| row.get(0),
    );

    let secret_bytes = match secret_res {
        Ok(hex_str) => hex::decode(hex_str).unwrap_or_else(|_| vec![42u8; 32]),
        Err(_) => {
            use std::time::SystemTime;
            let seed = format!("{:?}:{:?}", SystemTime::now(), uuid::Uuid::new_v4());
            let mut hasher = Sha256::new();
            hasher.update(seed.as_bytes());
            let sec = hasher.finalize().to_vec();
            let _ = conn.execute(
                "INSERT INTO system_meta (key, value) VALUES ('signing_secret', ?1)",
                params![hex::encode(&sec)],
            );
            println!("[SQLite] Initialized new persistent cryptographic audit signing key.");
            sec
        }
    };

    // 2. Get or initialize last event hash for cryptographic chain
    let last_hash_res: Result<String> = conn.query_row(
        "SELECT value FROM system_meta WHERE key = 'last_event_hash'",
        [],
        |row| row.get(0),
    );

    let last_hash = match last_hash_res {
        Ok(h) => h,
        Err(_) => {
            let genesis = "0000000000000000000000000000000000000000000000000000000000000000".to_string();
            let _ = conn.execute(
                "INSERT INTO system_meta (key, value) VALUES ('last_event_hash', ?1)",
                params![genesis],
            );
            genesis
        }
    };

    println!("[SQLite] Database initialized. Genesis hash: {}...", &last_hash[0..8]);
    Ok((conn, secret_bytes, last_hash))
}

pub fn record_event(
    conn: &mut Connection,
    secret: &[u8],
    last_hash: &mut String,
    order_id: &str,
    event_type: &str,
    actor: &str,
    details: Option<&str>,
) -> Result<OrderEvent> {
    let now = Utc::now();
    let timestamp = now.timestamp_millis();
    let iso_time = now.to_rfc3339();

    let (signature, new_hash) = generate_event_signature(
        secret,
        last_hash,
        timestamp,
        order_id,
        event_type,
        details,
    );

    let prev_h = last_hash.clone();
    *last_hash = new_hash.clone();

    // Persist event
    conn.execute(
        "INSERT INTO order_events (order_id, timestamp, iso_time, event_type, actor, details, prev_hash, signature)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![order_id, timestamp, iso_time, event_type, actor, details, prev_h, signature],
    )?;

    // Update last event hash in system_meta
    conn.execute(
        "UPDATE system_meta SET value = ?1 WHERE key = 'last_event_hash'",
        params![new_hash],
    )?;

    Ok(OrderEvent {
        timestamp,
        iso_time,
        event_type: event_type.to_string(),
        actor: actor.to_string(),
        details: details.map(|s| s.to_string()),
        prev_hash: prev_h,
        signature,
    })
}

pub fn create_order_with_event(
    conn: &mut Connection,
    secret: &[u8],
    last_hash: &mut String,
    mut order: Order,
    actor: &str,
    details: Option<&str>,
) -> Result<Order> {
    insert_order(conn, &order)?;
    let event = record_event(conn, secret, last_hash, &order.id, "CreatedInOffice", actor, details)?;
    order.events.push(event);
    Ok(order)
}

pub fn insert_order(conn: &mut Connection, order: &Order) -> Result<()> {
    conn.execute(
        "INSERT INTO orders (id, order_number, client_name, material, collection, color_code, base, total_buckets, bucket_weight, buckets_completed, status, delay_reason, token, created_at, accepted_at, mixing_started_at, mixing_duration_secs, ready_at, delivered_at, is_archived)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20)",
        params![
            order.id,
            order.order_number,
            order.client_name,
            order.material,
            order.collection,
            order.color_code,
            order.base,
            order.total_buckets,
            order.bucket_weight,
            order.buckets_completed,
            order.status.as_str(),
            order.delay_reason,
            order.token,
            order.created_at,
            order.accepted_at,
            order.mixing_started_at,
            order.mixing_duration_secs,
            order.ready_at,
            order.delivered_at,
            if order.is_archived { 1 } else { 0 }
        ],
    )?;
    Ok(())
}

pub fn update_order_status(
    conn: &mut Connection,
    id: &str,
    status: &OrderStatus,
    accepted_at: Option<i64>,
    ready_at: Option<i64>,
    delivered_at: Option<i64>,
    is_archived: bool,
) -> Result<()> {
    conn.execute(
        "UPDATE orders SET status = ?1, accepted_at = COALESCE(?2, accepted_at), ready_at = COALESCE(?3, ready_at), delivered_at = COALESCE(?4, delivered_at), is_archived = ?5 WHERE id = ?6",
        params![
            status.as_str(),
            accepted_at,
            ready_at,
            delivered_at,
            if is_archived { 1 } else { 0 },
            id
        ],
    )?;
    Ok(())
}

pub fn update_order_buckets(
    conn: &mut Connection,
    id: &str,
    buckets_completed: u32,
    status: &OrderStatus,
) -> Result<()> {
    conn.execute(
        "UPDATE orders SET buckets_completed = ?1, status = ?2 WHERE id = ?3",
        params![buckets_completed, status.as_str(), id],
    )?;
    Ok(())
}

pub fn update_order_pause(
    conn: &mut Connection,
    id: &str,
    status: &OrderStatus,
    delay_reason: Option<&str>,
) -> Result<()> {
    conn.execute(
        "UPDATE orders SET status = ?1, delay_reason = ?2 WHERE id = ?3",
        params![status.as_str(), delay_reason, id],
    )?;
    Ok(())
}

pub fn delete_order(conn: &mut Connection, id: &str) -> Result<()> {
    conn.execute("DELETE FROM orders WHERE id = ?1", params![id])?;
    Ok(())
}

pub fn load_active_orders(conn: &Connection) -> Result<HashMap<String, Order>> {
    let mut stmt = conn.prepare(
        "SELECT id, order_number, client_name, material, collection, color_code, base, total_buckets, bucket_weight, buckets_completed, status, delay_reason, token, created_at, accepted_at, mixing_started_at, mixing_duration_secs, ready_at, delivered_at, is_archived
         FROM orders WHERE is_archived = 0 ORDER BY created_at DESC"
    )?;

    let order_iter = stmt.query_map([], |row| {
        let id: String = row.get(0)?;
        let status_str: String = row.get(10)?;
        let is_archived_int: i32 = row.get(19)?;

        Ok(Order {
            id,
            order_number: row.get(1)?,
            client_name: row.get(2)?,
            material: row.get(3)?,
            collection: row.get(4)?,
            color_code: row.get(5)?,
            base: row.get(6)?,
            total_buckets: row.get(7)?,
            bucket_weight: row.get(8)?,
            buckets_completed: row.get(9)?,
            status: OrderStatus::from_str(&status_str),
            delay_reason: row.get(11)?,
            token: row.get(12)?,
            created_at: row.get(13)?,
            accepted_at: row.get(14)?,
            mixing_started_at: row.get(15)?,
            mixing_duration_secs: row.get(16)?,
            ready_at: row.get(17)?,
            delivered_at: row.get(18)?,
            is_archived: is_archived_int == 1,
            events: vec![],
        })
    })?;

    let mut map = HashMap::new();
    for o_res in order_iter {
        let mut order = o_res?;
        order.events = load_events_for_order(conn, &order.id)?;
        map.insert(order.id.clone(), order);
    }

    Ok(map)
}

pub fn load_order_by_token(conn: &Connection, token: &str) -> Result<Option<Order>> {
    let mut stmt = conn.prepare(
        "SELECT id, order_number, client_name, material, collection, color_code, base, total_buckets, bucket_weight, buckets_completed, status, delay_reason, token, created_at, accepted_at, mixing_started_at, mixing_duration_secs, ready_at, delivered_at, is_archived
         FROM orders WHERE token = ?1 LIMIT 1"
    )?;

    let mut rows = stmt.query([token])?;
    if let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        let status_str: String = row.get(10)?;
        let is_archived_int: i32 = row.get(19)?;

        let mut order = Order {
            id: id.clone(),
            order_number: row.get(1)?,
            client_name: row.get(2)?,
            material: row.get(3)?,
            collection: row.get(4)?,
            color_code: row.get(5)?,
            base: row.get(6)?,
            total_buckets: row.get(7)?,
            bucket_weight: row.get(8)?,
            buckets_completed: row.get(9)?,
            status: OrderStatus::from_str(&status_str),
            delay_reason: row.get(11)?,
            token: row.get(12)?,
            created_at: row.get(13)?,
            accepted_at: row.get(14)?,
            mixing_started_at: row.get(15)?,
            mixing_duration_secs: row.get(16)?,
            ready_at: row.get(17)?,
            delivered_at: row.get(18)?,
            is_archived: is_archived_int == 1,
            events: vec![],
        };
        order.events = load_events_for_order(conn, &id)?;
        Ok(Some(order))
    } else {
        Ok(None)
    }
}

pub fn load_archived_orders(conn: &Connection, limit: usize) -> Result<Vec<Order>> {
    let mut stmt = conn.prepare(
        "SELECT id, order_number, client_name, material, collection, color_code, base, total_buckets, bucket_weight, buckets_completed, status, delay_reason, token, created_at, accepted_at, mixing_started_at, mixing_duration_secs, ready_at, delivered_at, is_archived
         FROM orders WHERE is_archived = 1 ORDER BY delivered_at DESC LIMIT ?1"
    )?;

    let order_iter = stmt.query_map(params![limit as i64], |row| {
        let id: String = row.get(0)?;
        let status_str: String = row.get(10)?;
        let is_archived_int: i32 = row.get(19)?;

        Ok(Order {
            id,
            order_number: row.get(1)?,
            client_name: row.get(2)?,
            material: row.get(3)?,
            collection: row.get(4)?,
            color_code: row.get(5)?,
            base: row.get(6)?,
            total_buckets: row.get(7)?,
            bucket_weight: row.get(8)?,
            buckets_completed: row.get(9)?,
            status: OrderStatus::from_str(&status_str),
            delay_reason: row.get(11)?,
            token: row.get(12)?,
            created_at: row.get(13)?,
            accepted_at: row.get(14)?,
            mixing_started_at: row.get(15)?,
            mixing_duration_secs: row.get(16)?,
            ready_at: row.get(17)?,
            delivered_at: row.get(18)?,
            is_archived: is_archived_int == 1,
            events: vec![],
        })
    })?;

    let mut list = Vec::new();
    for o_res in order_iter {
        let mut order = o_res?;
        order.events = load_events_for_order(conn, &order.id)?;
        list.push(order);
    }

    Ok(list)
}

pub fn load_events_for_order(conn: &Connection, order_id: &str) -> Result<Vec<OrderEvent>> {
    let mut stmt = conn.prepare(
        "SELECT timestamp, iso_time, event_type, actor, details, prev_hash, signature
         FROM order_events WHERE order_id = ?1 ORDER BY timestamp ASC"
    )?;

    let iter = stmt.query_map(params![order_id], |row| {
        Ok(OrderEvent {
            timestamp: row.get(0)?,
            iso_time: row.get(1)?,
            event_type: row.get(2)?,
            actor: row.get(3)?,
            details: row.get(4)?,
            prev_hash: row.get(5)?,
            signature: row.get(6)?,
        })
    })?;

    let mut events = Vec::new();
    for e in iter {
        events.push(e?);
    }
    Ok(events)
}

pub fn save_push_subscription(conn: &mut Connection, order_id: &str, sub: &PushSubscription) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO push_subscriptions (order_id, endpoint, p256dh, auth)
         VALUES (?1, ?2, ?3, ?4)",
        params![order_id, sub.endpoint, sub.keys.p256dh, sub.keys.auth],
    )?;
    Ok(())
}

pub fn load_push_subscriptions(conn: &Connection) -> Result<HashMap<String, Vec<PushSubscription>>> {
    let mut stmt = conn.prepare("SELECT order_id, endpoint, p256dh, auth FROM push_subscriptions")?;
    let iter = stmt.query_map([], |row| {
        let order_id: String = row.get(0)?;
        let endpoint: String = row.get(1)?;
        let p256dh: String = row.get(2)?;
        let auth: String = row.get(3)?;
        Ok((order_id, PushSubscription {
            endpoint,
            keys: PushKeys { p256dh, auth },
        }))
    })?;

    let mut map: HashMap<String, Vec<PushSubscription>> = HashMap::new();
    for res in iter {
        let (order_id, sub) = res?;
        map.entry(order_id).or_default().push(sub);
    }
    Ok(map)
}
