import sqlite3
import datetime as dt
import secrets


def get_conn(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path):
    with get_conn(db_path) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS api_keys (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                key         TEXT    UNIQUE NOT NULL,
                name        TEXT    NOT NULL,
                email       TEXT,
                description TEXT,
                created     TEXT    NOT NULL,
                active      INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS request_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ts          TEXT    NOT NULL,
                key_id      INTEGER REFERENCES api_keys(id),
                endpoint    TEXT    NOT NULL,
                query_str   TEXT,
                status_code INTEGER,
                response_ms INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_log_ts     ON request_log(ts);
            CREATE INDEX IF NOT EXISTS idx_log_key_id ON request_log(key_id);
        """)


def lookup_key(db_path, key_str):
    """Return the key row dict if active, else None."""
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM api_keys WHERE key = ? AND active = 1", (key_str,)
        ).fetchone()
    return dict(row) if row else None


def log_request(db_path, key_id, endpoint, query_str, status_code, response_ms):
    ts = dt.datetime.utcnow().isoformat()
    with get_conn(db_path) as conn:
        conn.execute(
            """INSERT INTO request_log
               (ts, key_id, endpoint, query_str, status_code, response_ms)
               VALUES (?,?,?,?,?,?)""",
            (ts, key_id, endpoint, query_str, status_code, response_ms)
        )


def get_keys_with_stats(db_path):
    with get_conn(db_path) as conn:
        rows = conn.execute("""
            SELECT
                k.id, k.name, k.email, k.description, k.created, k.active,
                COUNT(r.id) AS total_requests,
                SUM(CASE WHEN r.ts >= datetime('now', '-7 days') THEN 1 ELSE 0 END) AS requests_7d,
                MAX(r.ts) AS last_seen
            FROM api_keys k
            LEFT JOIN request_log r ON r.key_id = k.id
            GROUP BY k.id
            ORDER BY k.created DESC
        """).fetchall()
    return [dict(r) for r in rows]


def get_recent_requests(db_path, limit=100):
    with get_conn(db_path) as conn:
        rows = conn.execute("""
            SELECT r.ts, k.name, r.endpoint, r.query_str, r.status_code, r.response_ms
            FROM request_log r
            LEFT JOIN api_keys k ON k.id = r.key_id
            ORDER BY r.ts DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return [dict(r) for r in rows]


def create_key(db_path, name, email=None, description=None):
    """Generate and store a new API key. Returns the key string."""
    key = secrets.token_urlsafe(32)
    created = dt.datetime.utcnow().isoformat()
    with get_conn(db_path) as conn:
        conn.execute(
            "INSERT INTO api_keys (key, name, email, description, created) VALUES (?,?,?,?,?)",
            (key, name, email, description, created)
        )
    return key


def deactivate_key(db_path, key_id):
    with get_conn(db_path) as conn:
        conn.execute("UPDATE api_keys SET active = 0 WHERE id = ?", (key_id,))
