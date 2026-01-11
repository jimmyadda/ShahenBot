# shahenbot_db.py
from datetime import date, datetime, timedelta, timezone
import secrets
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

 #Payments
    cur.execute(
        """
           CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            building_id INTEGER NOT NULL,
            tenant_id   INTEGER NOT NULL,

            amount_cents INTEGER NOT NULL,
            currency     TEXT NOT NULL DEFAULT 'ILS',

            method TEXT NOT NULL,     -- 'bank_transfer' / 'bit' / 'paybox' / ...
            status TEXT NOT NULL,     -- 'pending' / 'approved' / 'rejected'

            period_ym TEXT NULL,      -- אופציונלי: 'YYYY-MM' כדי לשייך תשלום לחודש

            proof_file_id   TEXT NOT NULL,  -- חובה כדי לאשר/לדחות
            proof_file_type TEXT NULL,      -- 'photo' / 'document'

            note TEXT NULL,

            created_at  TEXT NOT NULL DEFAULT (datetime('now')),
            approved_at TEXT NULL,
            approved_by TEXT NULL
        );
        """)
    
    cur.execute(
        """
            CREATE TABLE IF NOT EXISTS announcements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            building_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            target_group TEXT NOT NULL DEFAULT 'all', -- all/owners/renters
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
        """)
    #POOLs
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS polls (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        building_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        target_group TEXT NOT NULL DEFAULT 'all',  -- all/owners/renters
        is_anonymous INTEGER NOT NULL DEFAULT 1,   -- 1/0
        status TEXT NOT NULL DEFAULT 'open',       -- open/closed
        closes_at TEXT,                            -- ISO datetime optional
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
     """
    )

    cur.execute(
                """
        CREATE TABLE IF NOT EXISTS poll_options (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        poll_id INTEGER NOT NULL,
        option_text TEXT NOT NULL,
        FOREIGN KEY (poll_id) REFERENCES polls(id) ON DELETE CASCADE
        );
        """
        )
    
    cur.execute(
                """
            CREATE TABLE IF NOT EXISTS poll_votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            poll_id INTEGER NOT NULL,
            option_id INTEGER NOT NULL,
            tenant_id INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (poll_id) REFERENCES polls(id) ON DELETE CASCADE,
            FOREIGN KEY (option_id) REFERENCES poll_options(id) ON DELETE CASCADE
            );
        """
        )
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_poll_vote_tenant ON poll_votes(poll_id, tenant_id); """)   

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_payments_tenant_status
        ON payments(tenant_id, status); """)
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_payments_building_created
        ON payments(building_id, created_at);""")

    cur.execute(
        """        
        CREATE UNIQUE INDEX IF NOT EXISTS uq_payments_tenant_period
        ON payments(tenant_id, period_ym);""")

    cur.execute(
        """
       CREATE TABLE IF NOT EXISTS tenant_portal_tokens (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tenant_id INTEGER NOT NULL,
  token TEXT NOT NULL UNIQUE,
  expires_at TEXT NOT NULL,      -- ISO datetime
  used_at TEXT,                  -- ISO datetime (optional)
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
        """
    )

    cur.execute(
        """
     CREATE INDEX IF NOT EXISTS ix_tpt_tenant ON tenant_portal_tokens(tenant_id);
""")
    
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_tpt_token ON tenant_portal_tokens(token); """)
    
        # In case tickets existed before without tenant_id – add column if missing
    cur.execute("PRAGMA table_info(tickets)")
    cols = [r[1] for r in cur.fetchall()]
    if "tenant_id" not in cols:
        cur.execute("ALTER TABLE tickets ADD COLUMN tenant_id INTEGER")
    if "image_url" not in cols:
        cur.execute("ALTER TABLE tickets ADD COLUMN image_url TEXT")

    ensure_column(cur, "tickets", "building_id", "INTEGER")
    ensure_column(cur, "tenants", "building_id", "INTEGER")
    ensure_column(cur, "polls", "closed_at", "TEXT")
    ensure_column(cur, "polls", "sent_at", "TEXT")
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
    parking_slots: str = None,
    chat_id: int = None,
    building_id: int = None,
) -> dict:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO tenants
        (name, apartment, tenant_type, email, payment_type,
         next_payment_date, parking_slots, chat_id, building_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            building_id,
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
        SELECT id, building_id, name, apartment, tenant_type, email, payment_type,
               next_payment_date, parking_slots, chat_id
        FROM tenants
        WHERE id = ?
        """,
        (tenant_id,),
    )
    r = cur.fetchone()
    conn.close()
    if not r:
        return None

    return {
        "id": r[0],
        "building_id": r[1],
        "name": r[2],
        "apartment": r[3],
        "tenant_type": r[4],
        "email": r[5],
        "payment_type": r[6],
        "next_payment_date": r[7],
        "parking_slots": r[8],
        "chat_id": r[9],
    }

def get_tenant_by_chat_id_db(chat_id: int) -> dict | None:
    # Treat 0/None as not registered
    if not chat_id or int(chat_id) <= 0:
        return None

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, building_id, name, apartment, tenant_type, email, payment_type,
               next_payment_date, parking_slots, chat_id
        FROM tenants
        WHERE chat_id = ?
        """,
        (chat_id,),
    )
    r = cur.fetchone()
    conn.close()
    if not r:
        return None

    # building_id must be valid
    if not r[1] or int(r[1]) <= 0:
        return None

    return {
        "id": r[0], "building_id": r[1], "name": r[2], "apartment": r[3],
        "tenant_type": r[4], "email": r[5], "payment_type": r[6],
        "next_payment_date": r[7], "parking_slots": r[8], "chat_id": r[9],
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
    parking_slots: str,
    building_id: int,
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
            building_id = ?
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
            building_id,
            tenant_id,
        ),
    )
    conn.commit()
    conn.close()

    # ─────────── Tickets helpers ───────────

def update_tenant_name_db(tenant_id: int, name: str) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE tenants SET name = ? WHERE id = ?", (name, tenant_id))
    conn.commit()
    ok = cur.rowcount > 0
    conn.close()
    return ok

def get_tenants_summary_db(building_id: int | None = None) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor()

    q = """
    SELECT
      tn.id, tn.name, tn.apartment, tn.tenant_type, tn.email,
      tn.payment_type, tn.next_payment_date, tn.parking_slots,
      tn.chat_id, tn.building_id,
      b.street, b.number, b.city
    FROM tenants tn
    LEFT JOIN buildings b ON b.id = tn.building_id
    WHERE 1=1
    """
    params = []
    if building_id is not None:
      q += " AND tn.building_id = ?"
      params.append(building_id)

    q += " ORDER BY tn.building_id, tn.apartment, tn.name"

    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()

    out = []
    for r in rows:
        out.append({
            "id": r[0], "name": r[1], "apartment": r[2], "tenant_type": r[3],
            "email": r[4], "payment_type": r[5], "next_payment_date": r[6],
            "parking_slots": r[7], "chat_id": r[8], "building_id": r[9],
            "building_street": r[10], "building_number": r[11], "building_city": r[12],
        })
    return out

# ─────────── Ticket helpers ───────────

def create_ticket_db(chat_id: int, category: str, description: str, language: str = "he", image_url: str | None = None, status="open") -> dict:
    tenant = get_tenant_by_chat_id_db(chat_id)
    if not tenant:
        raise ValueError("not_registered")

    building_id = int(tenant["building_id"])
    if building_id <= 0:
        raise ValueError("not_registered")

    conn = get_connection()
    cur = conn.cursor()
    created_at = now_utc_iso()
    cur.execute(
        """
        INSERT INTO tickets (building_id, chat_id, category, description, language, status, created_at, image_url)
        VALUES (?, ?, ?, ?, ?, 'open', ?, ?)
        """,
        (building_id, chat_id, category, description, language, created_at, image_url),
    )
    conn.commit()
    tid = cur.lastrowid
    conn.close()
    return get_ticket_by_id_db(tid)

def get_tickets_db(
    limit: int = 100,
    status: str = None,
    category: str = None,
    search: str = None,
    building_id: int | None = None,
) -> list:
    conn = get_connection()
    cur = conn.cursor()

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

    if building_id is not None:
        query += " AND t.building_id = ?"
        params.append(building_id)

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

    query += " ORDER BY datetime(t.created_at) DESC LIMIT ?"
    params.append(limit)

    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    return [
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
        for r in rows
    ]

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

def find_open_ticket_by_category_db(building_id: int, category: str) -> dict | None:
    if not building_id or int(building_id) <= 0:
        return None

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, building_id, chat_id, category, description, status, created_at
        FROM tickets
        WHERE building_id=? AND status='open' AND category=?
        ORDER BY datetime(created_at) DESC
        LIMIT 1
        """,
        (building_id, category),
    )
    r = cur.fetchone()
    conn.close()
    if not r:
        return None
    return {
        "id": r[0],
        "building_id": r[1],
        "chat_id": r[2],
        "category": r[3],
        "description": r[4],
        "status": r[5],
        "created_at": r[6],
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

def get_buildings_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, city, street, number, name FROM buildings ORDER BY city, street, number")
    rows = cur.fetchall()
    conn.close()
    return [
        {"id": r[0], "city": r[1], "street": r[2], "number": r[3], "name": r[4]}
        for r in rows
    ]

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

def update_building_db(building_id: int, city: str | None, street: str, number: str, name: str | None, is_active: int = 1) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE buildings
        SET city = ?,
            street = ?,
            number = ?,
            name = ?,
            is_active = ?
        WHERE id = ?
        """,
        (city, street.strip(), str(number).strip(), name, int(is_active), building_id),
    )
    conn.commit()
    conn.close()
    return get_building_by_id_db(building_id)

def deactivate_building_db(building_id: int) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE buildings SET is_active = 0 WHERE id = ?", (building_id,))
    conn.commit()
    ok = cur.rowcount > 0
    conn.close()
    return ok

def ensure_column(cur, table: str, col: str, col_def: str):
    cur.execute(f"PRAGMA table_info({table})")
    cols = {r[1] for r in cur.fetchall()}
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}")

def backfill_building_ids_db(default_building_id: int):
    conn = get_connection()
    cur = conn.cursor()

    # tickets without building_id -> default
    cur.execute(
        "UPDATE tickets SET building_id = ? WHERE building_id IS NULL",
        (default_building_id,),
    )

    # tenants without building_id -> default
    cur.execute(
        "UPDATE tenants SET building_id = ? WHERE building_id IS NULL",
        (default_building_id,),
    )

    conn.commit()
    conn.close()

def resolve_building_by_street_number_db(street: str, number: str) -> dict | None:
    street = (street or "").strip()
    number = (number or "").strip()

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, city, street, number, name, is_active, created_at
        FROM buildings
        WHERE TRIM(LOWER(street)) = TRIM(LOWER(?))
          AND TRIM(number) = TRIM(?)
        LIMIT 1
        """,
        (street, number),
    )
    r = cur.fetchone()
    conn.close()

    if not r:
        return None

    return {
        "id": r[0],
        "city": r[1],
        "street": r[2],
        "number": r[3],
        "name": r[4],
        "is_active": r[5],
        "created_at": r[6],
    }

def get_tenants_by_building_apartment_db(
    building_id: int,
    apartment: str,
    only_without_chat: bool = True,
) -> list[dict]:
    apartment = (apartment or "").strip()

    conn = get_connection()
    cur = conn.cursor()

    sql = """
        SELECT id, name, apartment, tenant_type, email,building_id,chat_id
        FROM tenants
        WHERE building_id = ?
          AND TRIM(apartment) = TRIM(?)
    """
    params = [int(building_id), apartment]

    if only_without_chat:
        sql += " AND (chat_id IS NULL OR chat_id = '')"

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    tenants = []
    for r in rows:
        tenants.append({
            "id": r[0],            
            "name": r[1],
            "apartment": r[2],
            "tenant_type": r[3],
            "email": r[4],
            "email": r[5],
            "building_id": r[6],
            "chat_id": r[7]
        })

    return tenants

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

def get_tenants_due_this_month_db(building_id: int | None = None) -> list[dict]:    
    today = date.today().isoformat()          # 'YYYY-MM-DD'
    month_start = date.today().replace(day=1)
    next_month = (month_start + timedelta(days=32)).replace(day=1)
    month_end = next_month - timedelta(days=1)

    month_start_str = month_start.isoformat()
    month_end_str = month_end.isoformat()

    conn = get_connection()
    cur = conn.cursor()

    q = """
    SELECT id, name, apartment, next_payment_date, payment_type, building_id
    FROM tenants
    WHERE next_payment_date IS NOT NULL
      AND next_payment_date >= ?
      AND next_payment_date <= ?
    """
    params = [month_start_str, month_end_str]

    if building_id is not None:
        q += " AND building_id = ?"
        params.append(building_id)

    q += " ORDER BY next_payment_date ASC"

    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()

    return [{
        "id": r[0], "name": r[1], "apartment": r[2],
        "next_payment_date": r[3], "payment_type": r[4],
        "building_id": r[5],
    } for r in rows]

def compute_missing_tenant_fields(tenant: dict) -> list[str]:
    missing = []
    if not (tenant.get("tenant_type") or "").strip():
        missing.append("tenant_type")
    if not (tenant.get("email") or "").strip():
        missing.append("email")
    if not (tenant.get("payment_type") or "").strip():
        missing.append("payment_type")
    if not (tenant.get("next_payment_date") or "").strip():
        missing.append("next_payment_date")
    if not str(tenant.get("parking_slots") or "").strip():
        missing.append("parking_slots")
    return missing

# PAyments Helpers

def get_pending_payments_db(building_id: int | None = None):
    conn = get_connection()
    cur = conn.cursor()

    sql = """
    SELECT p.id, p.building_id, p.tenant_id, p.amount_cents, p.currency, p.method,
           p.status, p.period_ym, p.proof_file_id, p.proof_file_type, p.note, p.created_at,
           t.name, t.apartment, t.chat_id, t.next_payment_date
    FROM payments p
    JOIN tenants t ON t.id = p.tenant_id
    WHERE p.status='pending'
    """
    params = []

    if building_id:
        sql += " AND p.building_id=?"
        params.append(building_id)

    sql += " ORDER BY p.created_at DESC"

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    return [{
        "id": r[0], "building_id": r[1], "tenant_id": r[2],
        "amount_cents": r[3], "currency": r[4], "method": r[5],
        "status": r[6], "period_ym": r[7],
        "proof_file_id": r[8], "proof_file_type": r[9],
        "note": r[10], "created_at": r[11],
        "tenant_name": r[12], "apartment": r[13],
        "chat_id": r[14], "next_payment_date": r[15],
    } for r in rows]

def get_payment_by_id_db(payment_id: int) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            p.id,
            p.building_id,
            p.tenant_id,
            p.amount_cents,
            p.currency,
            p.method,
            p.status,
            p.period_ym,
            p.proof_file_id,
            p.proof_file_type,
            p.note,
            p.created_at,
            t.name,
            t.apartment,
            t.chat_id,
            t.next_payment_date
        FROM payments p
        JOIN tenants t ON t.id = p.tenant_id
        WHERE p.id=?
        """,
        (payment_id,),
    )
    r = cur.fetchone()
    conn.close()
    if not r:
        return None
    return {
        "id": r[0],
        "building_id": r[1],
        "tenant_id": r[2],
        "amount_cents": r[3],
        "currency": r[4],
        "method": r[5],
        "status": r[6],
        "period_ym": r[7],
        "proof_file_id": r[8],
        "proof_file_type": r[9],
        "note": r[10],
        "created_at": r[11],
        "tenant_name": r[12],
        "apartment": r[13],
        "chat_id": r[14],
        "next_payment_date": r[15],
    }

PAYMENT_WINDOW_DAYS = 14  # שבועיים

def tenant_has_pending_payment_db(tenant_id: int) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM payments WHERE tenant_id=? AND status='pending' LIMIT 1",
        (tenant_id,),
    )
    r = cur.fetchone()
    conn.close()
    return bool(r)    

def is_fully_registered(tenant: dict) -> bool:
    if not tenant:
        return False

    required = ["name", "apartment", "building_id", "chat_id"]
    for f in required:
        v = tenant.get(f)
        if v is None or str(v).strip() == "":
            return False

    return True

def should_add_payment_cta(tenant: dict):
    """
    Returns: (show_button: bool, reason: str | None)
    """
    if not is_fully_registered(tenant):
        return False, None

    next_date = tenant.get("next_payment_date")
    if not next_date:
        return False, None

    try:
        next_dt = date.fromisoformat(next_date)  # 'YYYY-MM-DD'
    except Exception:
        return False, None

    if tenant_has_pending_payment_db(tenant["id"]):
        return False, None

    today = date.today()

    if next_dt < today:
        return True, "יש תשלום ועד באיחור."
    if next_dt <= today + timedelta(days=PAYMENT_WINDOW_DAYS):
        return True, "תשלום ועד מתקרב."
    return False, None

def create_pending_payment_db(chat_id: int, amount_cents: int, method: str, period_ym: str | None = None) -> dict:
    tenant = get_tenant_by_chat_id_db(chat_id)
    if not tenant or not is_fully_registered(tenant):
        return {"ok": False, "error": "not_registered_fully"}

    if not period_ym:
        period_ym = date.today().strftime("%Y-%m")

    conn = get_connection()
    cur = conn.cursor()

    # Reuse existing pending for same month
    cur.execute(
        "SELECT id FROM payments WHERE tenant_id=? AND status='pending' AND period_ym=? LIMIT 1",
        (tenant["id"], period_ym),
    )
    r = cur.fetchone()
    if r:
        conn.close()
        return {"ok": True, "payment_id": r[0], "existing": True}

    try:
        cur.execute(
            """
            INSERT INTO payments (building_id, tenant_id, amount_cents, currency, method, status, period_ym, proof_file_id)
            VALUES (?, ?, ?, 'ILS', ?, 'pending', ?, 'TEMP')
            """,
            (tenant["building_id"], tenant["id"], int(amount_cents), method, period_ym),
        )
        conn.commit()
        payment_id = cur.lastrowid
        conn.close()
        return {"ok": True, "payment_id": payment_id}

    except sqlite3.IntegrityError as e:
        conn.rollback()
        # In case UNIQUE period_ym race, fetch again
        cur.execute(
            "SELECT id FROM payments WHERE tenant_id=? AND status='pending' AND period_ym=? LIMIT 1",
            (tenant["id"], period_ym),
        )
        r2 = cur.fetchone()
        conn.close()
        if r2:
            return {"ok": True, "payment_id": r2[0], "existing": True}
        return {"ok": False, "error": "integrity_error", "details": str(e)}

    except Exception as e:
        conn.rollback()
        conn.close()
        return {"ok": False, "error": "server_error", "details": str(e)}

def attach_payment_proof_db(payment_id: int, file_id: str, file_type: str) -> dict:
    if not file_id:
        return {"ok": False, "error": "missing_file_id"}

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE payments
        SET proof_file_id=?, proof_file_type=?
        WHERE id=? AND status='pending'
        """,
        (file_id, file_type, payment_id),
    )
    conn.commit()
    updated = cur.rowcount
    conn.close()

    if updated == 0:
        return {"ok": False, "error": "not_found_or_not_pending"}
    return {"ok": True}

def add_months(d: date, months: int) -> date:
    """
    Add months to date, clamping day to last day of target month.
    Example: Jan 31 + 1 month => Feb 28/29
    """
    months = int(months)
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1

    # days in month
    if m in (1, 3, 5, 7, 8, 10, 12):
        last_day = 31
    elif m in (4, 6, 9, 11):
        last_day = 30
    else:
        # February
        is_leap = (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0))
        last_day = 29 if is_leap else 28

    day = min(d.day, last_day)
    return date(y, m, day)

def set_next_payment_date_from_months_db(tenant_id: int, months: int) -> str:
    months = max(1, min(120, int(months)))  # הגנה
    new_dt = add_months(date.today(), months)

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE tenants SET next_payment_date=? WHERE id=?",
        (new_dt.isoformat(), tenant_id),
    )
    conn.commit()
    conn.close()
    return new_dt.isoformat()

def approve_payment_db(payment_id: int, approved_by: str = "admin") -> bool:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE payments
        SET status='approved', approved_at=datetime('now'), approved_by=?
        WHERE id=? AND status='pending'
        """,
        (approved_by, payment_id),
    )
    conn.commit()
    ok = (cur.rowcount or 0) > 0
    conn.close()
    return ok

def reject_payment_db(payment_id: int, note: str | None = None, approved_by: str = "admin") -> bool:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE payments
        SET status='rejected', approved_at=datetime('now'), approved_by=?, note=COALESCE(?, note)
        WHERE id=? AND status='pending'
        """,
        (approved_by, note, payment_id),
    )
    conn.commit()
    ok = (cur.rowcount or 0) > 0
    conn.close()
    return ok

def get_due_tenants_db(building_id: int | None, days_ahead: int = 0):
    conn = get_connection()
    cur = conn.cursor()

    sql = """
    SELECT id, name, apartment, next_payment_date, building_id
    FROM tenants
    WHERE next_payment_date IS NOT NULL
      AND date(next_payment_date) <= date('now', ?)
    """
    params = [f"+{int(days_ahead)} days"]

    if building_id:
        sql += " AND building_id=?"
        params.append(building_id)

    sql += " ORDER BY date(next_payment_date) ASC"

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [
        {"id": r[0], "name": r[1], "apartment": r[2], "next_payment_date": r[3], "building_id": r[4]}
        for r in rows
    ]

def get_payments_history_db(building_id: int | None, year: int | None = None, month: int | None = None):
    conn = get_connection()
    cur = conn.cursor()

    q = """
    SELECT p.id, t.name, t.apartment, t.building_id,
           p.amount_cents, p.currency, p.method, p.created_at
    FROM payments p
    JOIN tenants t ON t.id = p.tenant_id
    WHERE p.status='approved'
    """
    params = []

    if building_id:
        q += " AND t.building_id=?"
        params.append(building_id)

    if year:
        q += " AND strftime('%Y', p.created_at) = ?"
        params.append(str(year))
    if month:
        q += " AND strftime('%m', p.created_at) = ?"
        params.append(f"{int(month):02d}")

    q += " ORDER BY p.created_at DESC"

    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()

    return [{
        "id": r[0],
        "tenant_name": r[1],
        "apartment": r[2],
        "building_id": r[3],
        "amount_cents": r[4],
        "currency": r[5],
        "method": r[6],
        "created_at": r[7],
    } for r in rows]


#------- POLLS------

def get_staff_user_by_id_db(staff_user_id: int) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, username, role, building_id, created_at
        FROM staff_users
        WHERE id=?
        """,
        (staff_user_id,),
    )
    r = cur.fetchone()
    conn.close()
    if not r:
        return None
    return {
        "id": r[0],
        "username": r[1],
        "role": r[2],
        "building_id": r[3],
        "created_at": r[4],
    }

def create_poll_db(building_id: int, title: str, description: str, target_group: str, is_anonymous: int, closes_at: str | None, options: list[str]):
    title = (title or "").strip()
    description = (description or "").strip()
    target_group = (target_group or "all").strip()
    is_anonymous = 1 if int(is_anonymous or 0) else 0
    closes_at = (closes_at or "").strip() or None

    clean_opts = [o.strip() for o in (options or []) if (o or "").strip()]
    if len(clean_opts) < 2:
        return {"ok": False, "error": "need_2_options"}

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO polls(building_id, title, description, target_group, is_anonymous, closes_at)
        VALUES(?,?,?,?,?,?)
        """,
        (building_id, title, description, target_group, is_anonymous, closes_at),
    )
    poll_id = cur.lastrowid

    for opt in clean_opts:
        cur.execute("INSERT INTO poll_options(poll_id, option_text) VALUES(?,?)", (poll_id, opt))

    conn.commit()
    conn.close()
    return {"ok": True, "poll_id": poll_id}

def list_polls_db(building_id: int | None = None, status: str | None = None, limit: int = 100):
    conn = get_connection()
    cur = conn.cursor()

    sql = """
    SELECT id, building_id, title, description, target_group, is_anonymous, status, closes_at, sent_at, created_at
    FROM polls
    WHERE 1=1
    """
    params = []

    if building_id:
        sql += " AND building_id=?"
        params.append(building_id)

    if status:
        sql += " AND status=?"
        params.append(status)

    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(int(limit))

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    return [
        {"id": r[0], "building_id": r[1], "title": r[2], "description": r[3], "target_group": r[4],
         "is_anonymous": r[5], "status": r[6], "closes_at": r[7], "sent_at": r[8], "created_at": r[9]}
        for r in rows
    ]

def get_poll_with_options_db(poll_id: int):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, building_id, title, description, target_group, is_anonymous, status, closes_at, sent_at, created_at
        FROM polls WHERE id=?
        """,
        (poll_id,),
    )
    p = cur.fetchone()
    if not p:
        conn.close()
        return None

    cur.execute("SELECT id, option_text FROM poll_options WHERE poll_id=? ORDER BY id", (poll_id,))
    opts = cur.fetchall()

    conn.close()
    return {
        "id": p[0], "building_id": p[1], "title": p[2], "description": p[3],
        "target_group": p[4], "is_anonymous": p[5], "status": p[6],
        "closes_at": p[7],"sent_at": p[8],"created_at": p[9],
        "options": [{"id": r[0], "text": r[1]} for r in opts],
    }

def cast_vote_db(poll_id: int, option_id: int, tenant_id: int):
    """
    Returns:
      ok True/False
      error: already_voted / poll_closed / invalid_option
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT status FROM polls WHERE id=?", (poll_id,))
    r = cur.fetchone()
    if not r:
        conn.close()
        return {"ok": False, "error": "poll_not_found"}
    if r[0] != "open":
        conn.close()
        return {"ok": False, "error": "poll_closed"}

    cur.execute("SELECT 1 FROM poll_options WHERE id=? AND poll_id=?", (option_id, poll_id))
    if not cur.fetchone():
        conn.close()
        return {"ok": False, "error": "invalid_option"}

    try:
        cur.execute(
            "INSERT INTO poll_votes(poll_id, option_id, tenant_id) VALUES(?,?,?)",
            (poll_id, option_id, tenant_id),
        )
        conn.commit()
    except Exception:
        conn.close()
        return {"ok": False, "error": "already_voted"}

    conn.close()
    return {"ok": True}

def poll_results_db(poll_id: int):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT id, option_text FROM poll_options WHERE poll_id=? ORDER BY id", (poll_id,))
    opts = cur.fetchall()

    cur.execute(
        """
        SELECT option_id, COUNT(*)
        FROM poll_votes
        WHERE poll_id=?
        GROUP BY option_id
        """,
        (poll_id,),
    )
    counts = {row[0]: row[1] for row in cur.fetchall()}

    cur.execute("SELECT COUNT(*) FROM poll_votes WHERE poll_id=?", (poll_id,))
    total = int(cur.fetchone()[0] or 0)

    conn.close()

    out = []
    for oid, txt in opts:
        out.append({"option_id": oid, "text": txt, "votes": int(counts.get(oid, 0))})

    return {"poll_id": poll_id, "total_votes": total, "options": out}

def close_poll_db(poll_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE polls SET status='closed' WHERE id=?", (poll_id,))
    conn.commit()
    conn.close()
    return True

def mark_poll_sent_db(poll_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE polls SET sent_at=datetime('now') WHERE id=? AND (sent_at IS NULL OR sent_at='')", (poll_id,))
    conn.commit()
    changed = cur.rowcount
    conn.close()
    return changed > 0
#----------Announcement--------#

def create_announcement_db(building_id: int, title: str, body: str, target_group: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO announcements(building_id, title, body, target_group)
        VALUES(?,?,?,?)
        """,
        (building_id, title.strip(), body.strip(), target_group),
    )
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return new_id

def list_announcements_db(building_id: int | None = None, limit: int = 50):
    conn = get_connection()
    cur = conn.cursor()

    sql = "SELECT id, building_id, title, body, target_group, created_at FROM announcements"
    params = []
    if building_id:
        sql += " WHERE building_id=?"
        params.append(building_id)

    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(int(limit))

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [
        {"id": r[0], "building_id": r[1], "title": r[2], "body": r[3], "target_group": r[4], "created_at": r[5]}
        for r in rows
    ]

def get_recipients_chat_ids_by_group_db(building_id: int, target_group: str):
    conn = get_connection()
    cur = conn.cursor()

    sql = """
    SELECT chat_id
    FROM tenants
    WHERE building_id=?
      AND chat_id IS NOT NULL
      AND CAST(chat_id AS INTEGER) > 0
    """
    params = [building_id]

    if target_group == "owners":
        sql += " AND tenant_type='owner'"
    elif target_group == "renters":
        sql += " AND tenant_type='rent'"

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [int(r[0]) for r in rows if r[0]]


#--- tenants portal----#

def create_tenant_portal_token_db(tenant_id: int, ttl_minutes: int = 30) -> dict:
    """
    Creates a one-time-ish login token (we still allow reuse until expiry unless you enforce used_at).
    """
    token = secrets.token_urlsafe(32)
    expires = datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)
    expires_at = expires.isoformat()

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO tenant_portal_tokens(tenant_id, token, expires_at)
        VALUES(?,?,?)
        """,
        (tenant_id, token, expires_at),
    )
    conn.commit()
    conn.close()
    return {"token": token, "expires_at": expires_at}


def get_tenant_portal_token_db(token: str) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, tenant_id, token, expires_at, used_at, created_at
        FROM tenant_portal_tokens
        WHERE token=?
        """,
        (token,),
    )
    r = cur.fetchone()
    conn.close()
    if not r:
        return None
    return {
        "id": r[0],
        "tenant_id": r[1],
        "token": r[2],
        "expires_at": r[3],
        "used_at": r[4],
        "created_at": r[5],
    }


def mark_tenant_portal_token_used_db(token_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE tenant_portal_tokens SET used_at=datetime('now') WHERE id=?",
        (token_id,),
    )
    conn.commit()
    conn.close()


def is_token_expired(expires_at_iso: str) -> bool:
    try:
        # stored in UTC isoformat
        exp = datetime.fromisoformat(expires_at_iso.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) > exp
    except Exception:
        return True


def is_tenant_fully_registered(tenant: dict) -> bool:
    # התאמה  למינימום שיש לך: בניין + דירה + שם + chat
    if not tenant:
        return False
    if int(tenant.get("building_id") or 0) <= 0:
        return False
    if not (tenant.get("apartment") or "").strip():
        return False
    if not (tenant.get("name") or "").strip() or (tenant.get("name") or "").startswith("New Tenant"):
        return False
    if int(tenant.get("chat_id") or 0) <= 0:
        return False
    return True


# ------- Dashboard data fetchers (תתאים אם שמות הטבלאות אצלך שונים) -------

def list_tenant_tickets_db(chat_id: int, limit: int = 20):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, category, description, status, created_at
        FROM tickets
        WHERE chat_id=?
        ORDER BY id DESC
        LIMIT ?
        """,
        (chat_id, int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return [
        {"id": r[0], "category": r[1], "description": r[2], "status": r[3], "created_at": r[4]}
        for r in rows
    ]


def list_tenant_payments_db(tenant_id: int, limit: int = 20):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, amount_cents, currency, method, status, created_at
        FROM payments
        WHERE tenant_id=?
        ORDER BY id DESC
        LIMIT ?
        """,
        (tenant_id, int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return [
        {"id": r[0], "amount_cents": r[1], "currency": r[2], "method": r[3], "status": r[4], "created_at": r[5]}
        for r in rows
    ]


def list_building_announcements_db(building_id: int, limit: int = 5):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, title, body, target_group, created_at
        FROM announcements
        WHERE building_id=?
        ORDER BY id DESC
        LIMIT ?
        """,
        (building_id, int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return [
        {"id": r[0], "title": r[1], "body": r[2], "target_group": r[3], "created_at": r[4]}
        for r in rows
    ]