# shahenbot_db.py
from datetime import datetime, timezone
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from werkzeug.security import generate_password_hash, check_password_hash

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
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
            image_url TEXT,
            tenant_id INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ticket_watchers (
            ticket_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            PRIMARY KEY (ticket_id, chat_id)
        )
        """
    )

    cur.execute(
    """
    CREATE TABLE IF NOT EXISTS buildings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT,
        street TEXT NOT NULL,
        number TEXT NOT NULL,
        name TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TEXT
    )
    """
)

    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_buildings_unique
        ON buildings (COALESCE(city,''), street, number)
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS staff_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL,          -- 'super_admin' | 'building_admin'
            building_id INTEGER,         -- NULL for super_admin
            created_at TEXT,
            FOREIGN KEY(building_id) REFERENCES buildings(id)
        )
        """
    )


        # In case tickets existed before without tenant_id – add column if missing
    cur.execute("PRAGMA table_info(tickets)")
    cols = [r[1] for r in cur.fetchall()]
    if "tenant_id" not in cols:
        cur.execute("ALTER TABLE tickets ADD COLUMN tenant_id INTEGER")
    if "image_url" not in cols:
        cur.execute("ALTER TABLE tickets ADD COLUMN image_url TEXT")

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

# ─────────── Ticket helpers ───────────

def create_ticket_db(
    chat_id: int,
    category: str,
    description: str,
    language: str,
    status: str = "open",
    image_url: str | None = None,
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
                             status, created_at,image_url, tenant_id)
        VALUES (?, ?, ?, ?, ?, ?,?, ?)
        """,
        (chat_id, category, description, language, status, created_at,image_url, tenant_id),
    )

    conn.commit()
    ticket_id = cur.lastrowid
    conn.close()

    return get_ticket_by_id_db(ticket_id)

def get_tickets_db(
    limit: int = 100,
    status: str = None,
    category: str = None,
    search: str = None,
    building_id: int | None = None,
) -> list:
    """
    Return a list of tickets with optional filters.
    If building_id column does not exist yet, building filter is ignored (Step 1 safe).
    """
    conn = get_connection()
    cur = conn.cursor()

    # Detect if tickets table already has building_id column (Step 1 safe)
    cur.execute("PRAGMA table_info(tickets)")
    cols = {row[1] for row in cur.fetchall()}
    has_building_id = "building_id" in cols

    query = """
    SELECT
        t.id,
        t.chat_id,
        t.category,
        t.description,
        t.language,
        t.status,
        t.created_at,
        t.image_url,
        tn.id AS tenant_id,
        tn.name AS tenant_name,
        tn.apartment AS tenant_apartment
    FROM tickets t
    LEFT JOIN tenants tn ON t.chat_id = tn.chat_id
    WHERE 1=1
    """
    params = []

    # Optional building filter (only if column exists)
    if building_id is not None and has_building_id:
        query += " AND t.building_id = ?"
        params.append(building_id)

    # Optional filters
    if status and status != "all":
        query += " AND t.status = ?"
        params.append(status)

    if category and category != "all":
        query += " AND t.category = ?"
        params.append(category)

    if search:
        like = f"%{search}%"
        query += """
            AND (
                t.description LIKE ? OR
                t.category LIKE ? OR
                CAST(t.chat_id AS TEXT) LIKE ? OR
                COALESCE(tn.name,'') LIKE ? OR
                COALESCE(tn.apartment,'') LIKE ?
            )
        """
        params.extend([like, like, like, like, like])

    query += " ORDER BY datetime(t.created_at) DESC"
    query += " LIMIT ?"
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
                "image_url": r[7],
                "tenant_id": r[8],
                "tenant_name": r[9],
                "tenant_apartment": r[10],
            }
        )
    return tickets

def get_ticket_by_id_db(ticket_id: int) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, chat_id, category, description, language,
               status, created_at,image_url, tenant_id
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
        "image_url":r[7],
        "tenant_id": r[8],
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

    #chaid to tenant

def get_tenants_by_apartment_db(apartment: str, only_without_chat: bool = False) -> list:
    """
    Return tenants for a given apartment.
    If only_without_chat=True, returns only rows where chat_id IS NULL.
    """
    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT id, name, apartment, tenant_type, email,
               payment_type, next_payment_date, parking_slots, chat_id
        FROM tenants
        WHERE apartment = ?
    """
    params = [apartment]

    if only_without_chat:
        query += " AND (chat_id IS NULL OR chat_id = '')"

    query += " ORDER BY id"

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

def link_tenant_chat_db(tenant_id: int, chat_id: int) -> dict | None:    
    """
    Link a Telegram chat_id to a tenant.
    For safety, first clear this chat_id from any other tenant (unique mapping).
    """
    conn = get_connection()
    cur = conn.cursor()

    # Optional: ensure no other tenant keeps this chat_id
    cur.execute("UPDATE tenants SET chat_id = NULL WHERE chat_id = ?", (chat_id,))

    # Link to target tenant
    cur.execute(
        "UPDATE tenants SET chat_id = ? WHERE id = ?",
        (chat_id, tenant_id),
    )
    conn.commit()
    conn.close()

    return get_tenant_by_id_db(tenant_id)

def get_tickets_for_chat_db(chat_id: int) -> dict:
    """
    Return tickets created by this chat_id and tickets the chat_id is watching.
    """
    conn = get_connection()
    cur = conn.cursor()

    # Tickets created by this user
    cur.execute(
        """
        SELECT id, category, description, status, created_at
        FROM tickets
        WHERE chat_id = ?
        ORDER BY datetime(created_at) DESC
        """,
        (chat_id,),
    )
    own_rows = cur.fetchall()

    # Tickets the user is watching
    cur.execute(
        """
        SELECT t.id, t.category, t.description, t.status, t.created_at
        FROM ticket_watchers w
        JOIN tickets t ON t.id = w.ticket_id
        WHERE w.chat_id = ?
        ORDER BY datetime(t.created_at) DESC
        """,
        (chat_id,),
    )
    watch_rows = cur.fetchall()

    conn.close()

    own = [
        {
            "id": r[0],
            "category": r[1],
            "description": r[2],
            "status": r[3],
            "created_at": r[4],
        }
        for r in own_rows
    ]

    watching = [
        {
            "id": r[0],
            "category": r[1],
            "description": r[2],
            "status": r[3],
            "created_at": r[4],
        }
        for r in watch_rows
    ]

    return {"own": own, "watching": watching}

# ─────────── duplicate ticket helpers ───────────

def find_open_ticket_by_category_db(category: str) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, chat_id, category, description, language, status, created_at, image_url
        FROM tickets
        WHERE status = 'open' AND category = ?
        ORDER BY datetime(created_at) DESC
        LIMIT 1
        """,
        (category,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "id": row[0],
        "chat_id": row[1],
        "category": row[2],
        "description": row[3],
        "language": row[4],
        "status": row[5],
        "created_at": row[6],
        "image_url": row[7],
    }

def add_ticket_watcher_db(ticket_id: int, chat_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO ticket_watchers (ticket_id, chat_id)
        VALUES (?, ?)
        """,
        (ticket_id, chat_id),
    )
    conn.commit()
    conn.close()

def get_ticket_watchers_db(ticket_id: int) -> list[int]:    
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT chat_id FROM ticket_watchers WHERE ticket_id = ?",
        (ticket_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]

# ─────────── Building helpers ───────────

def create_building_db(city: str | None, street: str, number: str, name: str | None = None) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO buildings (city, street, number, name, is_active, created_at)
        VALUES (?, ?, ?, ?, 1, ?)
        """,
        (city, street.strip(), str(number).strip(), name, now_utc_iso()),
    )
    conn.commit()
    bid = cur.lastrowid
    conn.close()
    return get_building_by_id_db(bid)

def get_building_by_id_db(building_id: int) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, city, street, number, name, is_active, created_at FROM buildings WHERE id=?",
        (building_id,),
    )
    r = cur.fetchone()
    conn.close()
    if not r:
        return None
    return {"id": r[0], "city": r[1], "street": r[2], "number": r[3], "name": r[4], "is_active": r[5], "created_at": r[6]}

def list_buildings_db(limit: int = 500, search: str | None = None) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor()
    q = """
      SELECT id, city, street, number, name, is_active, created_at
      FROM buildings
      WHERE 1=1
    """
    params = []
    if search:
        like = f"%{search}%"
        q += " AND (street LIKE ? OR number LIKE ? OR COALESCE(city,'') LIKE ? OR COALESCE(name,'') LIKE ?)"
        params += [like, like, like, like]
    q += " ORDER BY street, number LIMIT ?"
    params.append(limit)
    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()
    return [
        {"id": r[0], "city": r[1], "street": r[2], "number": r[3], "name": r[4], "is_active": r[5], "created_at": r[6]}
        for r in rows
    ]


# ─────────── Staff helpers ───────────

def create_staff_user_db(username: str, password: str, role: str, building_id: int | None) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO staff_users (username, password_hash, role, building_id, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (username.strip().lower(), generate_password_hash(password), role, building_id, now_utc_iso()),
    )
    conn.commit()
    uid = cur.lastrowid
    conn.close()
    return get_staff_user_by_id_db(uid)

def get_staff_user_by_username_db(username: str) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, username, password_hash, role, building_id FROM staff_users WHERE username=?",
        (username.strip().lower(),),
    )
    r = cur.fetchone()
    conn.close()
    if not r:
        return None
    return {"id": r[0], "username": r[1], "password_hash": r[2], "role": r[3], "building_id": r[4]}

def get_staff_user_by_id_db(user_id: int) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, username, password_hash, role, building_id FROM staff_users WHERE id=?", (user_id,))
    r = cur.fetchone()
    conn.close()
    if not r:
        return None
    return {"id": r[0], "username": r[1], "password_hash": r[2], "role": r[3], "building_id": r[4]}

def verify_staff_password(user: dict, password: str) -> bool:
    return check_password_hash(user["password_hash"], password)

def list_staff_users_db(limit: int = 200) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT u.id, u.username, u.role, u.building_id,
               b.street, b.number, COALESCE(b.city,''), COALESCE(b.name,'')
        FROM staff_users u
        LEFT JOIN buildings b ON b.id = u.building_id
        ORDER BY u.role, u.username
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "id": r[0],
            "username": r[1],
            "role": r[2],
            "building_id": r[3],
            "building_street": r[4],
            "building_number": r[5],
            "building_city": r[6],
            "building_name": r[7],
        }
        for r in rows
    ]
