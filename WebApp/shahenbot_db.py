# shahenbot_db.py
from datetime import datetime, timezone
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).with_name("shahenbot.db")


def get_connection():
    """Return a new SQLite connection."""
    return sqlite3.connect(DB_PATH)

def init_db():
    """Create tables if they don't exist."""
    conn = get_connection()
    cur = conn.cursor()

    # User settings table (language per chat_id)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_settings (
            chat_id INTEGER PRIMARY KEY,
            language TEXT NOT NULL
        )
        """
    )
        # Tenants table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tenants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            apartment TEXT,
            tenant_type TEXT,          -- 'owner' / 'rent'
            email TEXT,
            payment_type TEXT,         -- 'monthly' / 'standing_order' / etc.
            next_payment_date TEXT,    -- ISO date string 'YYYY-MM-DD'
            parking_slots TEXT,
            chat_id INTEGER            -- Telegram chat id (optional)
        )
        """
    )
    # Tickets table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            category TEXT NOT NULL,
            description TEXT NOT NULL,
            language TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            tenant_id INTEGER
        )
        """
    )
        # In case tickets existed before without tenant_id – add column if missing
    cur.execute("PRAGMA table_info(tickets)")
    cols = [r[1] for r in cur.fetchall()]
    if "tenant_id" not in cols:
        cur.execute("ALTER TABLE tickets ADD COLUMN tenant_id INTEGER")

    conn.commit()
    conn.close()

def get_user_language_db(chat_id: int, default_lang: str = "he") -> str:
    """
    Return the language for this chat_id.
    If not found, insert with default_lang and return it.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT language FROM user_settings WHERE chat_id = ?", (chat_id,))
    row = cur.fetchone()

    if row:
        lang = row[0]
    else:
        lang = default_lang
        cur.execute(
            "INSERT INTO user_settings (chat_id, language) VALUES (?, ?)",
            (chat_id, lang),
        )
        conn.commit()

    conn.close()
    return lang

def set_user_language_db(chat_id: int, lang: str):
    """
    Set/update language for this chat_id.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO user_settings (chat_id, language)
        VALUES (?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET language = excluded.language
        """,
        (chat_id, lang),
    )
    conn.commit()
    conn.close()
# ─────────── Tenant helpers ───────────

def create_tenant_db(
    name: str,
    apartment: str = None,
    tenant_type: str = None,
    email: str = None,
    payment_type: str = None,
    next_payment_date: str = None,
    parking_slots: str  = None,
    chat_id: int = None,
) -> dict:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO tenants
        (name, apartment, tenant_type, email, payment_type,
         next_payment_date, parking_slots, chat_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            name,
            apartment,
            tenant_type,
            email,
            payment_type,
            next_payment_date,
            parking_slots,
            chat_id,
        ),
    )
    conn.commit()
    tenant_id = cur.lastrowid
    conn.close()

    return get_tenant_by_id_db(tenant_id)


def get_tenant_by_id_db(tenant_id: int) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, name, apartment, tenant_type, email,
               payment_type, next_payment_date, parking_slots, chat_id
        FROM tenants
        WHERE id = ?
        """,
        (tenant_id,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None

    return {
        "id": row[0],
        "name": row[1],
        "apartment": row[2],
        "tenant_type": row[3],
        "email": row[4],
        "payment_type": row[5],
        "next_payment_date": row[6],
        "parking_slots": row[7],
        "chat_id": row[8],
    }


def get_tenant_by_chat_id_db(chat_id: int) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, name, apartment, tenant_type, email,
               payment_type, next_payment_date, parking_slots, chat_id
        FROM tenants
        WHERE chat_id = ?
        """,
        (chat_id,),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "id": row[0],
        "name": row[1],
        "apartment": row[2],
        "tenant_type": row[3],
        "email": row[4],
        "payment_type": row[5],
        "next_payment_date": row[6],
        "parking_slots": row[7],
        "chat_id": row[8],
    }


def get_tenants_db(limit: int = 200, search: str | None = None) -> list:
    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT id, name, apartment, tenant_type, email,
               payment_type, next_payment_date, parking_slots, chat_id
        FROM tenants
        WHERE 1=1
    """
    params = []

    if search:
        query += """
            AND (
                name LIKE ?
                OR apartment LIKE ?
                OR email LIKE ?
            )
        """
        like = f"%{search}%"
        params.extend([like, like, like])

    query += " ORDER BY apartment, name LIMIT ?"
    params.append(limit)

    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    tenants = []
    for r in rows:
        tenants.append(
            {
                "id": r[0],
                "name": r[1],
                "apartment": r[2],
                "tenant_type": r[3],
                "email": r[4],
                "payment_type": r[5],
                "next_payment_date": r[6],
                "parking_slots": r[7],
                "chat_id": r[8],
            }
        )
    return tenants


def update_tenant_db(
    tenant_id: int,
    name: str,
    apartment: str,
    tenant_type: str,
    email: str,
    payment_type: str,
    next_payment_date: str,
    parking_slots: str | None,
    chat_id: int | None,
):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE tenants
        SET name = ?,
            apartment = ?,
            tenant_type = ?,
            email = ?,
            payment_type = ?,
            next_payment_date = ?,
            parking_slots = ?,
            chat_id = ?
        WHERE id = ?
        """,
        (
            name,
            apartment,
            tenant_type,
            email,
            payment_type,
            next_payment_date,
            parking_slots,
            chat_id,
            tenant_id,
        ),
    )
    conn.commit()
    conn.close()
    # ─────────── Tickets helpers ───────────

def create_ticket_db(
    chat_id: int,
    category: str,
    description: str,
    language: str,
    status: str = "open",
) -> dict:
    """
    Create a new ticket and return its data as a dict.
    If tenant exists with same chat_id, link tenant_id.
    """
    conn = get_connection()
    cur = conn.cursor()

    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # Find tenant by chat_id if exists
    cur.execute("SELECT id FROM tenants WHERE chat_id = ?", (chat_id,))
    trow = cur.fetchone()
    tenant_id = trow[0] if trow else None

    cur.execute(
        """
        INSERT INTO tickets (chat_id, category, description, language,
                             status, created_at, tenant_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (chat_id, category, description, language, status, created_at, tenant_id),
    )

    conn.commit()
    ticket_id = cur.lastrowid
    conn.close()

    return get_ticket_by_id_db(ticket_id)


def get_tickets_db(limit: int = 100, status: str = None,
                   category: str = None, search: str = None) -> list:
    """
    Return a list of tickets with optional filters.
    """
    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT id, chat_id, category, description, language,
               status, created_at, tenant_id
        FROM tickets
        WHERE 1=1
    """
    params = []

    if status and status != "all":
        query += " AND status = ?"
        params.append(status)

    if category and category != "all":
        query += " AND category LIKE ?"
        params.append(f"%{category}%")

    if search:
        query += """
            AND (
                description LIKE ?
                OR category LIKE ?
                OR CAST(chat_id AS TEXT) LIKE ?
            )
        """
        like = f"%{search}%"
        params.extend([like, like, like])

    query += " ORDER BY datetime(created_at) DESC LIMIT ?"
    params.append(limit)

    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    tickets = []
    for r in rows:
        tickets.append(
            {
                "id": r[0],
                "chat_id": r[1],
                "category": r[2],
                "description": r[3],
                "language": r[4],
                "status": r[5],
                "created_at": r[6],
                "tenant_id": r[7],
            }
        )
    return tickets


def get_ticket_by_id_db(ticket_id: int) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, chat_id, category, description, language,
               status, created_at, tenant_id
        FROM tickets
        WHERE id = ?
        """,
        (ticket_id,),
    )
    r = cur.fetchone()
    conn.close()

    if not r:
        return None

    return {
        "id": r[0],
        "chat_id": r[1],
        "category": r[2],
        "description": r[3],
        "language": r[4],
        "status": r[5],
        "created_at": r[6],
        "tenant_id": r[7],
    }


def update_ticket_status_db(ticket_id: int, status: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE tickets SET status = ? WHERE id = ?",
        (status, ticket_id),
    )
    conn.commit()
    conn.close()


def update_ticket_description_db(ticket_id: int, description: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE tickets SET description = ? WHERE id = ?",
        (description, ticket_id),
    )
    conn.commit()
    conn.close()