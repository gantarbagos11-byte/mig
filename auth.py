import hashlib
import re
import hmac
import secrets
import sqlite3
import time
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "auth.db"
SESSION_COOKIE = "migsock_session"
DEFAULT_SESSION_TTL = 12 * 60 * 60
SESSION_TTL_MIN = 5 * 60
SESSION_TTL_MAX = 30 * 24 * 60 * 60

# Bootstrap admin requested for this build. Only the salted scrypt hash is stored.
BOOTSTRAP_ADMIN_USERNAME = "chikovalen"
BOOTSTRAP_ADMIN_SALT = bytes.fromhex("e67a08f02b97b6b2135fe8b445e365a9")
BOOTSTRAP_ADMIN_HASH = bytes.fromhex("14bf4d770defe098e4197e22527bc6d54e4f8444de07e1fc5250a25af2ef662d")

SESSIONS = {}


def _connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_auth_db():
    with _connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY COLLATE NOCASE,
                password_salt BLOB NOT NULL,
                password_hash BLOB NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at REAL NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS devices (
                device_id TEXT PRIMARY KEY,
                username TEXT NOT NULL COLLATE NOCASE,
                secret_hash TEXT NOT NULL,
                label TEXT NOT NULL DEFAULT 'Perangkat',
                user_agent TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                created_at REAL NOT NULL,
                approved_at REAL,
                approved_by TEXT,
                last_seen_at REAL,
                FOREIGN KEY(username) REFERENCES users(username) ON DELETE CASCADE
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_devices_username ON devices(username COLLATE NOCASE)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_devices_status ON devices(status)")
        conn.execute(
            "INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)",
            ("session_ttl_seconds", str(DEFAULT_SESSION_TTL)),
        )
        row = conn.execute(
            "SELECT username FROM users WHERE username = ? COLLATE NOCASE",
            (BOOTSTRAP_ADMIN_USERNAME,),
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO users(username,password_salt,password_hash,role,enabled,created_at) VALUES(?,?,?,?,?,?)",
                (BOOTSTRAP_ADMIN_USERNAME, BOOTSTRAP_ADMIN_SALT, BOOTSTRAP_ADMIN_HASH, "admin", 1, time.time()),
            )
        conn.commit()


def _hash_password(password: str, salt: bytes | None = None):
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=2**14, r=8, p=1, dklen=32)
    return salt, digest


def verify_password(password: str, salt: bytes, expected: bytes) -> bool:
    _, actual = _hash_password(password, salt)
    return hmac.compare_digest(actual, expected)


def create_user(username: str, password: str, role: str = "user"):
    username = username.strip()
    role = role if role in {"user", "admin"} else "user"
    if not username or len(username) > 64:
        raise ValueError("Username tidak valid")
    if len(password) < 8:
        raise ValueError("Password minimal 8 karakter")
    salt, digest = _hash_password(password)
    with _connect() as conn:
        try:
            conn.execute(
                "INSERT INTO users(username,password_salt,password_hash,role,enabled,created_at) VALUES(?,?,?,?,?,?)",
                (username, salt, digest, role, 1, time.time()),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            raise ValueError("Username sudah terdaftar")


def list_users():
    with _connect() as conn:
        rows = conn.execute(
            "SELECT username, role, enabled, created_at FROM users ORDER BY role DESC, username COLLATE NOCASE"
        ).fetchall()
    return [dict(r) for r in rows]


def authenticate(username: str, password: str):
    with _connect() as conn:
        row = conn.execute(
            "SELECT username,password_salt,password_hash,role,enabled FROM users WHERE username = ? COLLATE NOCASE",
            (username.strip(),),
        ).fetchone()
    if not row or not row["enabled"]:
        return None
    if not verify_password(password, row["password_salt"], row["password_hash"]):
        return None
    return {"username": row["username"], "role": row["role"]}


def get_session_ttl():
    try:
        with _connect() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key=?", ("session_ttl_seconds",)).fetchone()
        value = int(row[0]) if row else DEFAULT_SESSION_TTL
    except Exception:
        value = DEFAULT_SESSION_TTL
    return max(SESSION_TTL_MIN, min(SESSION_TTL_MAX, value))


def set_session_ttl(seconds: int):
    seconds = int(seconds)
    if seconds < SESSION_TTL_MIN or seconds > SESSION_TTL_MAX:
        raise ValueError("Durasi session harus antara 5 menit dan 30 hari")
    with _connect() as conn:
        conn.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("session_ttl_seconds", str(seconds)),
        )
        conn.commit()
    return seconds



def _device_secret_hash(secret: str) -> str:
    # Device secrets are generated from 32 bytes of browser-side cryptographic randomness.
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def register_or_check_device(username: str, device_id: str, secret: str, label: str = "Perangkat", user_agent: str = ""):
    """Return approved/pending/rejected/revoked; never auto-approve member devices."""
    username = str(username or "").strip()
    device_id = str(device_id or "").strip()
    secret = str(secret or "")
    label = str(label or "Perangkat").strip()[:100] or "Perangkat"
    user_agent = str(user_agent or "")[:300]
    if not username or not re.fullmatch(r"[A-Za-z0-9_-]{16,100}", device_id):
        raise ValueError("Identitas perangkat tidak valid")
    if len(secret) < 40 or len(secret) > 200:
        raise ValueError("Kredensial perangkat tidak valid")
    secret_hash = _device_secret_hash(secret)
    now = time.time()
    with _connect() as conn:
        row = conn.execute("SELECT * FROM devices WHERE device_id=?", (device_id,)).fetchone()
        if row is None:
            conn.execute(
                """INSERT INTO devices(device_id,username,secret_hash,label,user_agent,status,created_at,last_seen_at)
                   VALUES(?,?,?,?,?,'pending',?,?)""",
                (device_id, username, secret_hash, label, user_agent, now, now),
            )
            conn.commit()
            return "pending"
        if row["username"].casefold() != username.casefold():
            return "invalid"
        if not hmac.compare_digest(str(row["secret_hash"]), secret_hash):
            return "invalid"
        conn.execute(
            "UPDATE devices SET last_seen_at=?, label=?, user_agent=? WHERE device_id=?",
            (now, label, user_agent, device_id),
        )
        conn.commit()
        return str(row["status"])


def list_devices(status: str = ""):
    query = """SELECT device_id, username, label, user_agent, status, created_at,
                      approved_at, approved_by, last_seen_at
               FROM devices"""
    args = []
    if status in {"pending", "approved", "rejected", "revoked"}:
        query += " WHERE status=?"
        args.append(status)
    query += " ORDER BY CASE status WHEN 'pending' THEN 0 WHEN 'approved' THEN 1 ELSE 2 END, created_at DESC"
    with _connect() as conn:
        rows = conn.execute(query, args).fetchall()
    return [dict(row) for row in rows]


def set_device_status(device_id: str, status: str, admin_username: str = ""):
    if status not in {"approved", "rejected", "revoked"}:
        raise ValueError("Status perangkat tidak valid")
    now = time.time()
    with _connect() as conn:
        row = conn.execute("SELECT username,status FROM devices WHERE device_id=?", (device_id,)).fetchone()
        if not row:
            raise ValueError("Perangkat tidak ditemukan")
        conn.execute(
            """UPDATE devices SET status=?, approved_at=?, approved_by=? WHERE device_id=?""",
            (status, now if status == "approved" else None,
             admin_username if status == "approved" else None, device_id),
        )
        conn.commit()
        username = row["username"]
    # A rejected/revoked device must lose all sessions immediately.
    if status != "approved":
        for token, session in list(SESSIONS.items()):
            if session.get("device_id") == device_id:
                SESSIONS.pop(token, None)
    return {"device_id": device_id, "username": username, "status": status}


def delete_device(device_id: str):
    device_id = str(device_id or "").strip()
    with _connect() as conn:
        row = conn.execute(
            "SELECT username FROM devices WHERE device_id=?", (device_id,)
        ).fetchone()
        if not row:
            raise ValueError("Perangkat tidak ditemukan")
        conn.execute("DELETE FROM devices WHERE device_id=?", (device_id,))
        conn.commit()
    # Removing a device also immediately invalidates any sessions tied to it.
    for token, session in list(SESSIONS.items()):
        if session.get("device_id") == device_id:
            SESSIONS.pop(token, None)
    return {"device_id": device_id, "username": row["username"], "deleted": True}


def delete_user_devices(username: str):
    with _connect() as conn:
        rows = conn.execute("SELECT device_id FROM devices WHERE username=? COLLATE NOCASE", (username,)).fetchall()
        ids = {row["device_id"] for row in rows}
        conn.execute("DELETE FROM devices WHERE username=? COLLATE NOCASE", (username,))
        conn.commit()
    for token, session in list(SESSIONS.items()):
        if session.get("device_id") in ids:
            SESSIONS.pop(token, None)


def _cleanup_sessions():
    now = time.time()
    for token, session in list(SESSIONS.items()):
        if session["expires"] <= now:
            SESSIONS.pop(token, None)


def create_session(user, device_id=None):
    _cleanup_sessions()
    token = secrets.token_urlsafe(32)
    SESSIONS[token] = {
        "username": user["username"],
        "role": user["role"],
        "device_id": device_id,
        "expires": time.time() + get_session_ttl(),
    }
    return token


def get_session(token):
    if not token:
        return None
    _cleanup_sessions()
    session = SESSIONS.get(token)
    if not session:
        return None
    with _connect() as conn:
        row = conn.execute(
            "SELECT enabled,role FROM users WHERE username = ? COLLATE NOCASE",
            (session["username"],),
        ).fetchone()
        device_ok = True
        if row and row["role"] != "admin":
            device_id = session.get("device_id")
            device = conn.execute(
                "SELECT status,username FROM devices WHERE device_id=?",
                (device_id,),
            ).fetchone() if device_id else None
            device_ok = bool(device and device["status"] == "approved"
                             and device["username"].casefold() == session["username"].casefold())
    if not row or not row["enabled"] or not device_ok:
        SESSIONS.pop(token, None)
        return None
    session["role"] = row["role"]
    session["expires"] = time.time() + get_session_ttl()
    return {"username": session["username"], "role": session["role"]}


def destroy_session(token):
    if token:
        SESSIONS.pop(token, None)


def set_enabled(username: str, enabled: bool):
    with _connect() as conn:
        row = conn.execute("SELECT role FROM users WHERE username = ? COLLATE NOCASE", (username,)).fetchone()
        if not row:
            raise ValueError("User tidak ditemukan")
        if row["role"] == "admin" and not enabled:
            admins = conn.execute("SELECT COUNT(*) FROM users WHERE role='admin' AND enabled=1").fetchone()[0]
            if admins <= 1:
                raise ValueError("Admin aktif terakhir tidak boleh dinonaktifkan")
        conn.execute("UPDATE users SET enabled=? WHERE username = ? COLLATE NOCASE", (1 if enabled else 0, username))
        conn.commit()
    # Invalidate active sessions for the account immediately when disabled.
    if not enabled:
        for token, session in list(SESSIONS.items()):
            if session["username"].casefold() == username.casefold():
                SESSIONS.pop(token, None)


def delete_user(username: str):
    with _connect() as conn:
        row = conn.execute("SELECT role FROM users WHERE username = ? COLLATE NOCASE", (username,)).fetchone()
        if not row:
            raise ValueError("User tidak ditemukan")
        if row["role"] == "admin":
            admins = conn.execute("SELECT COUNT(*) FROM users WHERE role='admin'").fetchone()[0]
            if admins <= 1:
                raise ValueError("Admin terakhir tidak boleh dihapus")
        conn.execute("DELETE FROM devices WHERE username = ? COLLATE NOCASE", (username,))
        conn.execute("DELETE FROM users WHERE username = ? COLLATE NOCASE", (username,))
        conn.commit()
    for token, session in list(SESSIONS.items()):
        if session["username"].casefold() == username.casefold():
            SESSIONS.pop(token, None)


def change_password(username: str, new_password: str):
    if len(new_password) < 8:
        raise ValueError("Password minimal 8 karakter")
    salt, digest = _hash_password(new_password)
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE users SET password_salt=?, password_hash=? WHERE username = ? COLLATE NOCASE",
            (salt, digest, username),
        )
        if cur.rowcount != 1:
            raise ValueError("User tidak ditemukan")
        conn.commit()
    for token, session in list(SESSIONS.items()):
        if session["username"].casefold() == username.casefold():
            SESSIONS.pop(token, None)
