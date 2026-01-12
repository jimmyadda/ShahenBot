# WebApp/app.py
from datetime import date
from functools import wraps
import logging
import os
import sqlite3
from flask import Response, flash, session, abort
from dotenv import load_dotenv
import requests
import uuid
from werkzeug.utils import secure_filename

from flask import (
    Flask,
    jsonify,
    request,
    render_template,
    redirect,
    url_for,
)
from shahenbot_db import (
    approve_payment_db,
    attach_payment_proof_db,
    cast_vote_db,
    compute_missing_tenant_fields,
    create_announcement_db,
    create_pending_payment_db,
    create_poll_db,
    create_tenant_portal_token_db,
    get_buildings_db,
    get_due_tenants_db,
    get_payment_by_id_db,
    get_payments_history_db,
    get_pending_payments_db,
    get_poll_with_options_db,
    get_recipients_chat_ids_by_group_db,
    get_tenant_by_id_db,
    get_tenant_portal_token_db,
    get_tenants_by_building_apartment_db,
    get_tenants_due_this_month_db,
    get_tenants_summary_db,
    init_db,
    get_user_language_db,
    is_fully_registered,
    is_tenant_fully_registered,
    is_token_expired,
    list_announcements_db,
    list_building_announcements_db,
    list_polls_db,
    list_tenant_payments_db,
    list_tenant_tickets_db,
    mark_poll_sent_db,
    mark_tenant_portal_token_used_db,
    poll_results_db,
    reject_payment_db,
    resolve_building_by_street_number_db,
    set_next_payment_date_from_months_db,
    set_user_language_db,
    create_ticket_db,
    get_tickets_db,
    update_tenant_name_db,
    update_ticket_status_db,
    update_ticket_description_db,
    get_ticket_by_id_db,
    get_tenants_db,
    create_tenant_db,
    update_tenant_db,
    get_tenants_by_apartment_db,
    get_tenant_by_chat_id_db,       
    link_tenant_chat_db,
    find_open_ticket_by_category_db,
    add_ticket_watcher_db,              
    get_ticket_watchers_db,
    get_tickets_for_chat_db,
    create_building_db,
    list_buildings_db,
    get_building_by_id_db,
    create_staff_user_db,
    get_staff_user_by_username_db,
    get_staff_user_by_id_db,
    verify_staff_password,
    list_staff_users_db,
    update_building_db, 
    deactivate_building_db  ,
    backfill_building_ids_db,
        get_tenant_by_chat_id_db,
    should_add_payment_cta,
)


UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), "static", "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

# Initialize Flask app
app = Flask(__name__)

app.secret_key = os.getenv("FLASK_SECRET", "change_me_please")


# Initialize DB tables on startup
init_db()

def ensure_super_admin():
    username = os.getenv("SUPERADMIN_USER")
    password = os.getenv("SUPERADMIN_PASS")
    if not username or not password:
        return

    existing = get_staff_user_by_username_db(username)
    if not existing:
        create_staff_user_db(username, password, "super_admin", None)

# call after init_db()
init_db()
ensure_super_admin()


def send_telegram_message(chat_id: int, text: str, buttons: list | None = None):
    """
    buttons example:
    [
      [{"text": "💳 תשלום ועד", "callback_data": "pay_open"}]
    ]
    """
    if not BOT_TOKEN:
        print("BOT_TOKEN not set, cannot send Telegram messages")
        return

    payload = {"chat_id": chat_id, "text": text}
    if buttons:
        payload["reply_markup"] = {"inline_keyboard": buttons}

    try:
        resp = requests.post(
            f"{TELEGRAM_API}/sendMessage",
            json=payload,
            timeout=10,
        )
        if not resp.ok:
            print("Telegram sendMessage error:", resp.status_code, resp.text)
    except Exception as e:
        print("Telegram sendMessage exception:", e)

# User Helper
def current_user():
    uid = session.get("staff_user_id")
    return get_staff_user_by_id_db(uid) if uid else None

def require_login():
    u = current_user()
    if not u:
        return redirect(url_for("login"))
    return u

def require_super_admin():
    u = require_login()
    if not isinstance(u, dict):
        return u
    if u["role"] != "super_admin":
        abort(403)
    return u


def get_staff_scope():
    staff_user_id = session.get("staff_user_id")
    if not staff_user_id:
        return None, None, None

    staff = get_staff_user_by_id_db(int(staff_user_id))
    if not staff:
        return None, None, None

    role = staff.get("role")
    if role == "super_admin":
        bid = request.args.get("building_id", type=int)
        return staff, role, bid  # None => all buildings
    elif role == "building_admin":
        bid = staff.get("building_id")
        return staff, role, int(bid) if bid else None
    return staff, role, None

def admin_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        staff, role, bid = get_staff_scope()
        if not staff:
            return redirect(url_for("login"))
        return view(*args, **kwargs)
    return wrapped

def scoped_building_id(u: dict) -> int | None:
    # super admin sees all buildings (None = no filter)
    return None if u["role"] == "super_admin" else u["building_id"]

@app.get("/login")
def login():
    if current_user():
        return redirect(url_for("admin_dashboard"))
    return render_template("login.html", error=None)

@app.post("/login")
def login_post():
    username = request.form.get("username", "")
    password = request.form.get("password", "")

    user = get_staff_user_by_username_db(username)
    if not user or not verify_staff_password(user, password):
        return render_template("login.html", error="Invalid username or password")

    session["staff_user_id"] = user["id"]
    return redirect(url_for("admin_dashboard"))

@app.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
def home():
    return redirect(url_for("admin_dashboard"))

# ───────────────────────────────────────────────
#   API: GET USER LANGUAGE
# ───────────────────────────────────────────────
@app.get("/api/user/<int:chat_id>/language")
def api_get_language(chat_id: int):
    lang = get_user_language_db(chat_id, default_lang="he")
    return jsonify({"chat_id": chat_id, "language": lang})


# ───────────────────────────────────────────────
#   API: SET USER LANGUAGE
# ───────────────────────────────────────────────
@app.post("/api/user/<int:chat_id>/language")
def api_set_language(chat_id: int):
    data = request.get_json(silent=True) or {}
    lang = data.get("language")

    if not lang:
        return jsonify({"error": "Missing 'language' field"}), 400

    if lang not in ("he", "en", "fr"):
        return jsonify({"error": "Invalid language"}), 400

    set_user_language_db(chat_id, lang)
    return jsonify({"chat_id": chat_id, "language": lang})


# ───────────────────────────────────────────────
#   API: CREATE TICKET
# ───────────────────────────────────────────────
@app.post("/api/tickets")
def api_create_ticket():
    data = request.get_json(silent=True) or {}

    chat_id = data.get("chat_id")
    category = data.get("category")
    description = data.get("description")
    language = data.get("language", "he")
    image_url = data.get("image_url")  # NEW

    if not chat_id or not isinstance(chat_id, int):
        return jsonify({"error": "Invalid or missing 'chat_id' (int required)"}), 400
    if not category:
        return jsonify({"error": "Missing 'category'"}), 400
    if not description:
        return jsonify({"error": "Missing 'description'"}), 400

    ticket = create_ticket_db(
        chat_id=chat_id,
        category=category,
        description=description,
        language=language,
        image_url=image_url,
        status="open",
    )

    return jsonify(ticket), 201

@app.get("/api/tickets/check_duplicate")
def api_check_duplicate():
    building_id = request.args.get("building_id", type=int)
    category = request.args.get("category", type=str)

    if not building_id or not category:
        return jsonify({"error": "missing_fields"}), 400

    t = find_open_ticket_by_category_db(building_id, category)
    if t:
        return jsonify({"duplicate": True, "ticket": t}), 200
    return jsonify({"duplicate": False, "ticket": None}), 200

@app.post("/api/tickets/<int:ticket_id>/watchers")
def api_add_ticket_watcher(ticket_id: int):
    data = request.get_json(silent=True) or {}
    chat_id = data.get("chat_id")
    if not isinstance(chat_id, int):
        return jsonify({"error": "invalid_chat_id"}), 400

    # Require that this chat_id belongs to a registered tenant
    tenant = get_tenant_by_chat_id_db(chat_id)
    if not tenant:
        return jsonify({"error": "not_registered"}), 403

    add_ticket_watcher_db(ticket_id, chat_id)
    return jsonify({"ok": True})

# ───────────────────────────────────────────────
#   API: LIST TICKETS
# ───────────────────────────────────────────────
@app.get("/api/tickets")
def api_get_tickets():
    try:
        limit = int(request.args.get("limit", 100))
    except ValueError:
        limit = 100

    status = request.args.get("status")  # optional
    category = request.args.get("category")  # optional
    search = request.args.get("search")  # optional

    tickets = get_tickets_db(
        limit=limit,
        status=status,
        category=category,
        search=search,
    )
    return jsonify({"tickets": tickets})

@app.get("/api/tickets/by_chat/<int:chat_id>")
def api_tickets_by_chat(chat_id: int):
    """
    Return tickets opened by this chat_id or watched by this chat_id.
    """
    data = get_tickets_for_chat_db(chat_id)
    return jsonify(data)

# ───────────────────────────────────────────────
#   API: UPDATE TICKET DESCRIPTION (for Telegram edit)
#   POST /api/tickets/<ticket_id>/description
#   JSON: { "chat_id": 123, "description": "new text" }
# ───────────────────────────────────────────────
@app.post("/api/tickets/<int:ticket_id>/description")
def api_update_ticket_description(ticket_id: int):
    data = request.get_json(silent=True) or {}
    chat_id = data.get("chat_id")
    description = data.get("description")

    if not chat_id or not isinstance(chat_id, int):
        return jsonify({"error": "Invalid or missing 'chat_id'"}), 400
    if not description:
        return jsonify({"error": "Missing 'description'"}), 400

    ticket = get_ticket_by_id_db(ticket_id)
    if not ticket:
        return jsonify({"error": "Ticket not found"}), 404

    # simple protection: only the owner chat_id can edit
    if ticket["chat_id"] != chat_id:
        return jsonify({"error": "Not allowed to edit this ticket"}), 403
    
    # prevent editing closed tickets
    if ticket["status"] == "closed":
        return jsonify({"error": "ticket_closed"}), 400
    
    update_ticket_description_db(ticket_id, description)
    updated = get_ticket_by_id_db(ticket_id)

    return jsonify(updated), 200

@app.post("/api/upload_image")
def api_upload_image():
    """
    Receive an image file from Telegram bot, save it, return its URL.
    """
    if "file" not in request.files:
        return jsonify({"error": "no_file"}), 400

    f = request.files["file"]
    if f.filename == "":
        return jsonify({"error": "empty_filename"}), 400

    ext = os.path.splitext(f.filename)[1] or ".jpg"
    filename = secure_filename(f"{uuid.uuid4().hex}{ext}")
    save_path = os.path.join(UPLOAD_FOLDER, filename)
    f.save(save_path)

    # public URL (assuming /static is served)
    url = url_for("static", filename=f"uploads/{filename}", _external=True)
    return jsonify({"url": url})

# ───────────────────────────────────────────────
#   ADMIN DASHBOARD (HTML) – TICKETS
# ───────────────────────────────────────────────
@app.get("/admin")
def admin_dashboard():
    u = require_login()
    if not isinstance(u, dict):
        return u

    status = request.args.get("status") or ""
    category = request.args.get("category") or ""
    search = request.args.get("search") or ""
    limit = int(request.args.get("limit") or "100")

    building_filter = scoped_building_id(u)
    tenants = get_tenants_summary_db(building_filter)
    due_tenants = get_tenants_due_this_month_db(building_filter)
    buildings = list_buildings_db()
    tenants_missing = []
    for t in tenants:
        miss = compute_missing_tenant_fields(t)
        if miss:
            tenants_missing.append({**t, "missing": miss})

    tickets = get_tickets_db(
        limit=limit,
        status=status if status else None,
        category=category if category else None,
        search=search if search else None,
        building_id=building_filter,   # <-- add this param in DB func (Step 2 may adjust)
    )

    return render_template(
        "admin.html",
        tickets=tickets,
        status=status,
        category=category,
        search=search,
        tenants=tenants,
        tenants_missing=tenants_missing,
        due_tenants=due_tenants,
        buildings=buildings,
        limit=limit,
        current_user=u,
    )

# ───────────────────────────────────────────────
#   ADMIN: UPDATE TICKET STATUS + Telegram notify
# ───────────────────────────────────────────────
@app.post("/admin/tickets/<int:ticket_id>/status")
def admin_update_status(ticket_id):
    new_status = request.form.get("status")

    old_ticket = get_ticket_by_id_db(ticket_id)
    old_status = old_ticket["status"] if old_ticket else None

    update_ticket_status_db(ticket_id, new_status)
    ticket = get_ticket_by_id_db(ticket_id)

    # Notify only if status actually changed
    if ticket and old_status != new_status:
        chat_id_reporter = ticket["chat_id"]
        watchers = get_ticket_watchers_db(ticket_id)

        recipients = set(watchers)
        if chat_id_reporter:
            recipients.add(chat_id_reporter)

        # Simple text in Hebrew for now, can be multilingual later
        category = ticket["category"]
        desc = ticket["description"]
        status_txt = new_status

        notify_text = (
            f"עדכון דיווח #{ticket_id}:\n"
            f"קטגוריה: {category}\n"
            f"סטטוס חדש: {status_txt}\n\n"
            f"תיאור:\n{desc}"
        )

        #for cid in recipients:
        #    send_telegram_message(cid, notify_text)

        for cid in recipients:
            text = notify_text
            buttons = None

            tenant = get_tenant_by_chat_id_db(cid)
            show_pay, reason = should_add_payment_cta(tenant) if tenant else (False, None)

            if show_pay:
                text += f"\n\n💡 {reason}"
                buttons = [[{"text": "💳 תשלום ועד", "callback_data": "pay_open"}]]

            send_telegram_message(cid, text, buttons=buttons)

    return redirect(url_for("admin_dashboard"))

# ───────────────────────────────────────────────
#   ADMIN: TENANTS LIST + EDIT
# ───────────────────────────────────────────────
@app.get("/admin/tenants")
def admin_tenants():
    search = request.args.get("search", "").strip()
    tenants = get_tenants_db(limit=300, search=search)
    buildings = list_buildings_db()
    return render_template(
        "tenants.html",
        tenants=tenants,
        buildings =buildings,
        search=search,
    )

@app.post("/admin/tenants/add")
def admin_add_tenant():
    name = request.form.get("name", "").strip()
    apartment = request.form.get("apartment", "").strip()
    tenant_type = request.form.get("tenant_type") or None
    email = request.form.get("email", "").strip() or None
    payment_type = request.form.get("payment_type") or None
    next_payment_date = request.form.get("next_payment_date") or None
    parking = request.form.get("parking_slots", "").strip()
    chat_id = request.form.get("chat_id") or None
    building_id = int(request.form.get("building_id") or 0)

    parking = ",".join(s.strip() for s in parking.split(",") if s.strip())
    chat_id = int(chat_id) if chat_id else None

    if not name:
        return redirect(url_for("admin_tenants"))

    create_tenant_db(
        name=name,
        apartment=apartment,
        tenant_type=tenant_type,
        email=email,
        payment_type=payment_type,
        next_payment_date=next_payment_date,
        parking_slots=parking,
        building_id=building_id,
        chat_id=chat_id,
    )
    return redirect(url_for("admin_tenants"))

@app.post("/admin/tenants/<int:tenant_id>/update")
def admin_update_tenant(tenant_id: int):
    name = request.form.get("name", "").strip()
    apartment = request.form.get("apartment", "").strip()
    tenant_type = request.form.get("tenant_type") or None
    email = request.form.get("email", "").strip() or None
    payment_type = request.form.get("payment_type") or None
    next_payment_date = request.form.get("next_payment_date") or None
    parking_slots = request.form.get("parking_slots") or None
    building_id = int(request.form.get("building_id") or 0)

    parking_slots = parking_slots if parking_slots else None

    if not name:
        return redirect(url_for("admin_tenants"))

    update_tenant_db(
        tenant_id=tenant_id,
        name=name,
        apartment=apartment,
        tenant_type=tenant_type,
        email=email,
        payment_type=payment_type,
        next_payment_date=next_payment_date,
        parking_slots=parking_slots,
        building_id=building_id,
    )

    return redirect(url_for("admin_tenants"))
# ───────────────────────────────────────────────
#   API: TENANTS CHATID
# ───────────────────────────────────────────────
@app.get("/api/tenants/by_apartment/<apartment>")
def api_tenants_by_apartment(apartment: str):
    """
    Return tenants for a given apartment.
    Optional query param: only_without_chat=1
    """
    only_without_chat = request.args.get("only_without_chat") == "1"
    tenants = get_tenants_by_apartment_db(apartment, only_without_chat=only_without_chat)
    return jsonify({"tenants": tenants})

@app.get("/api/tenants/by_chat/<int:chat_id>")
def api_tenant_by_chat(chat_id: int):
    t = get_tenant_by_chat_id_db(chat_id)
    return jsonify({"tenant": t}), 200

@app.post("/api/tenants/<int:tenant_id>/link_chat")
def api_link_tenant_chat(tenant_id: int):
    data = request.get_json(silent=True) or {}
    chat_id = data.get("chat_id")

    if not isinstance(chat_id, int):
        return jsonify({"error": "invalid_chat_id"}), 400

    tenant = link_tenant_chat_db(tenant_id, chat_id)
    if not tenant:
        return jsonify({"error": "tenant_not_found"}), 404

    return jsonify(tenant), 200

# super admin   
@app.get("/admin/buildings")
def admin_buildings():
    u = require_super_admin()
    if not isinstance(u, dict):
        return u

    q = request.args.get("q") or ""
    buildings = list_buildings_db(search=q if q else None)
    return render_template("buildings.html", buildings=buildings, q=q, current_user=u)

@app.post("/admin/buildings")
def admin_buildings_create():
    u = require_super_admin()
    if not isinstance(u, dict):
        return u

    city = request.form.get("city") or None
    street = request.form.get("street") or ""
    number = request.form.get("number") or ""
    name = request.form.get("name") or None

    if not street.strip() or not number.strip():
        return redirect(url_for("admin_buildings"))

    create_building_db(city, street, number, name)
    return redirect(url_for("admin_buildings"))

@app.post("/admin/buildings/<int:building_id>/update")
def admin_buildings_update(building_id: int):
    u = require_super_admin()
    if not isinstance(u, dict):
        return u

    city = request.form.get("city") or None
    street = (request.form.get("street") or "").strip()
    number = (request.form.get("number") or "").strip()
    name = request.form.get("name") or None
    is_active = 1 if request.form.get("is_active") == "1" else 0

    if not street or not number:
        return redirect(url_for("admin_buildings"))

    try:
        update_building_db(building_id, city, street, number, name, is_active=is_active)
    except Exception:
        # Most likely UNIQUE constraint conflict (same city/street/number already exists)
        return redirect(url_for("admin_buildings"))

    return redirect(url_for("admin_buildings"))

@app.post("/admin/buildings/<int:building_id>/delete")
def admin_buildings_delete(building_id: int):
    u = require_super_admin()
    if not isinstance(u, dict):
        return u

    deactivate_building_db(building_id)
    return redirect(url_for("admin_buildings"))

# Building staff 
@app.get("/admin/staff")
def admin_staff():
    u = require_super_admin()
    if not isinstance(u, dict):
        return u

    staff = list_staff_users_db()
    buildings = list_buildings_db(limit=500)
    return render_template("staff.html", staff=staff, buildings=buildings, current_user=u, error=None)

@app.post("/admin/staff")
def admin_staff_create():
    u = require_super_admin()
    if not isinstance(u, dict):
        return u

    username = (request.form.get("username") or "").strip()
    password = (request.form.get("password") or "").strip()
    role = request.form.get("role") or "building_admin"
    building_id = request.form.get("building_id")
    print(username,password,building_id)
    
    buildings = list_buildings_db(limit=500)
    staff = list_staff_users_db()

    if not username or not password:
        return render_template("staff.html", staff=staff, buildings=buildings, current_user=u, error="Missing username/password")

    if role == "building_admin":
        if not building_id:
            return render_template("staff.html", staff=staff, buildings=buildings, current_user=u, error="building_id required for building_admin")
        building_id = int(building_id)
    else:
        building_id = None


    logger = logging.getLogger(__name__)

    try:
        create_staff_user_db(username, password, role, building_id)
    except sqlite3.IntegrityError as e:
        # UNIQUE constraint failed: staff_users.username
        logger.exception("Staff create integrity error")
        return render_template(
            "staff.html",
            staff=staff,
            buildings=buildings,
            current_user=u,
            error=f"Username already exists: {username}",
        )
    except Exception as e:
        logger.exception("Staff create unexpected error")
        return render_template(
            "staff.html",
            staff=staff,
            buildings=buildings,
            current_user=u,
            error=f"Error: {type(e).__name__}: {e}",
        )

    return redirect(url_for("admin_staff"))

@app.get("/admin/migrate/backfill_building")
def admin_backfill_building_get():
    u = require_super_admin()
    if not isinstance(u, dict):
        return u

    building_id = request.args.get("building_id", type=int)
    if not building_id:
        return "Missing building_id. Example: /admin/migrate/backfill_building?building_id=1", 400

    backfill_building_ids_db(building_id)
    return redirect(url_for("admin_dashboard"))

@app.route("/api/buildings/resolve", methods=["POST"])
def api_resolve_building_route():
    data = request.get_json(silent=True) or {}

    street = data.get("street")
    number = data.get("number")

    if not street or not number:
        return jsonify({"error": "missing_fields"}), 400

    building = resolve_building_by_street_number_db(street, number)

    if not building:
        return jsonify({"error": "not_found"}), 404

    return jsonify(building), 200

@app.route("/api/tenants/by_building_apartment", methods=["GET"])
def api_tenants_by_building_apartment():
    building_id = request.args.get("building_id", type=int)
    apartment = request.args.get("apartment", "")
    only_without_chat = request.args.get("only_without_chat", "1") == "1"

    if not building_id or not str(apartment).strip():
        return jsonify({"error": "missing_fields"}), 400

    tenants = get_tenants_by_building_apartment_db(
        building_id=building_id,
        apartment=apartment,
        only_without_chat=only_without_chat,
    )

    return jsonify({"tenants": tenants}), 200



# ───────────────────────────────────────────────
#   API: TENANTS PAYMENTS
# ───────────────────────────────────────────────

@app.post("/api/payments/create_pending")
def api_payments_create_pending():
    data = request.get_json(force=True) or {}
    chat_id = int(data.get("chat_id") or 0)
    amount_cents = int(data.get("amount_cents") or 0)
    method = (data.get("method") or "bank_transfer").strip()
    period_ym = (data.get("period_ym") or "").strip() or None

    res = create_pending_payment_db(chat_id=chat_id, amount_cents=amount_cents, method=method, period_ym=period_ym)

    if not res.get("ok"):
        # 400 only for not_registered_fully, else 500
        if res.get("error") == "not_registered_fully":
            return jsonify(res), 400
        return jsonify(res), 500

    return jsonify(res), 200

@app.post("/api/payments/<int:payment_id>/attach_proof")
def api_payments_attach_proof(payment_id):
    data = request.get_json(force=True) or {}
    file_id = (data.get("file_id") or "").strip()
    file_type = (data.get("file_type") or "").strip()

    res = attach_payment_proof_db(payment_id, file_id, file_type)
    if not res.get("ok"):
        return jsonify(res), 400
    return jsonify(res), 200

@app.get("/admin/payments")
def admin_payments():
    # super admin can view all; optional building_id filter
    building_id = request.args.get("building_id", type=int)  # None => all buildings
    year = request.args.get("year", type=int)
    month = request.args.get("month", type=int)

    buildings = get_buildings_db()

    payments = get_pending_payments_db(building_id)
    due_now = get_due_tenants_db(building_id, days_ahead=0)
    due_soon = get_due_tenants_db(building_id, days_ahead=14)
    history = get_payments_history_db(building_id, year, month)

    total_sum = sum((p["amount_cents"] or 0) for p in history) / 100.0

    return render_template(
        "admin_payments.html",
        buildings=buildings,
        building_id=building_id,
        payments=payments,
        due_now=due_now,
        due_soon=due_soon,
        history=history,
        total_sum=total_sum,
        year=year,
        month=month,
    )

@app.post("/admin/payments/<int:payment_id>/approve")
def admin_approve_payment(payment_id):
    months = request.form.get("months", type=int)
    if not months:
        flash("חובה להזין מספר חודשים לתשלום הבא", "danger")
        return redirect(url_for("admin_payments"))

    p = get_payment_by_id_db(payment_id)
    if not p:
        flash("תשלום לא נמצא", "danger")
        return redirect(url_for("admin_payments"))

    if not p.get("proof_file_id") or p["proof_file_id"] == "TEMP":
        flash("לא ניתן לאשר ללא אסמכתא", "danger")
        return redirect(url_for("admin_payments"))

    ok = approve_payment_db(payment_id, approved_by="admin")
    if not ok:
        flash("לא הצלחתי לאשר (אולי כבר טופל)", "warning")
        return redirect(url_for("admin_payments"))

    new_next = set_next_payment_date_from_months_db(p["tenant_id"], months)

    # אופציונלי: הודעה לדייר
    if p.get("chat_id"):
        amount = (p["amount_cents"] or 0) / 100
        txt = (
            f"✅ התשלום אושר\n"
            f"דירה: {p.get('apartment')}\n"
            f"סכום: {amount:.2f} {p.get('currency','ILS')}\n"
            f"תשלום הבא עודכן ל: {new_next}"
        )
        send_telegram_message(p["chat_id"], txt)

    flash("התשלום אושר ועודכן תשלום הבא", "success")
    return redirect(url_for("admin_payments"))


@app.post("/admin/payments/<int:payment_id>/reject")
def admin_reject_payment(payment_id):
    note = (request.form.get("note") or "").strip()

    p = get_payment_by_id_db(payment_id)
    if not p:
        flash("תשלום לא נמצא", "danger")
        return redirect(url_for("admin_payments"))

    if not p.get("proof_file_id") or p["proof_file_id"] == "TEMP":
        flash("לא ניתן לדחות ללא אסמכתא", "danger")
        return redirect(url_for("admin_payments"))

    ok = reject_payment_db(payment_id, note=note or None, approved_by="admin")
    if not ok:
        flash("לא הצלחתי לדחות (אולי כבר טופל)", "warning")
        return redirect(url_for("admin_payments"))

    if p.get("chat_id"):
        txt = "❌ התשלום נדחה."
        if note:
            txt += f"\nסיבה: {note}"
        txt += "\n\nאפשר לשלוח שוב אסמכתא ברורה יותר."
        buttons = [[{"text": "💳 תשלום ועד", "callback_data": "pay_open"}]]
        send_telegram_message(p["chat_id"], txt, buttons=buttons)

    flash("התשלום נדחה", "warning")
    return redirect(url_for("admin_payments"))


def tg_get_file_path(file_id: str) -> str | None:
    r = requests.get(f"{TELEGRAM_API}/getFile", params={"file_id": file_id}, timeout=10)
    if not r.ok:
        return None
    j = r.json() or {}
    return (j.get("result") or {}).get("file_path")

@app.get("/admin/payments/<int:payment_id>/proof")
def admin_payment_proof(payment_id):
    p = get_payment_by_id_db(payment_id)
    if not p:
        abort(404)

    file_id = p.get("proof_file_id")
    if not file_id or file_id == "TEMP":
        abort(404)

    file_path = tg_get_file_path(file_id)
    if not file_path:
        abort(404)

    file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
    r = requests.get(file_url, stream=True, timeout=20)
    if not r.ok:
        abort(404)

    content_type = r.headers.get("Content-Type", "application/octet-stream")
    return Response(r.iter_content(chunk_size=8192), content_type=content_type)

@app.post("/api/tenants/auto_register")
def api_tenants_auto_register():
    data = request.json or {}
    building_id = int(data.get("building_id", 0))
    apartment = (data.get("apartment") or "").strip()
    chat_id = int(data.get("chat_id", 0))

    if building_id <= 0 or not apartment or chat_id <= 0:
        return jsonify({"error": "missing_fields"}), 400

    # Already linked?
    existing = get_tenant_by_chat_id_db(chat_id)
    if existing:
        return jsonify({"tenant": existing}), 200

    # Create new tenant row for this person
    tenant = create_tenant_db(
        name=f"New Tenant ({apartment})",
        apartment=apartment,
        chat_id=chat_id,
        building_id=building_id,
    )
    return jsonify({"tenant": tenant}), 200

@app.post("/api/tenants/<int:tenant_id>/name")
def api_update_tenant_name(tenant_id: int):
    data = request.json or {}
    name = (data.get("name") or "").strip()

    if not name:
        return jsonify({"error": "missing_name"}), 400

    ok = update_tenant_name_db(tenant_id, name)
    if not ok:
        return jsonify({"error": "not_found"}), 404

    return jsonify({"ok": True}), 200

#--------Announcement----#
@app.get("/admin/announcements")
@admin_required
def admin_announcements():
    staff, role, building_id_scope = get_staff_scope()

    buildings = get_buildings_db() if role == "super_admin" else []
    items = list_announcements_db(building_id_scope)

    return render_template(
        "admin_announcements.html",
        staff=staff,
        role=role,
        buildings=buildings,
        building_id=building_id_scope,
        items=items,
    )


@app.post("/admin/announcements/create")
@admin_required
def admin_announcements_create():
    staff, role, building_id_scope = get_staff_scope()

    building_id = request.form.get("building_id", type=int)
    if role == "building_admin":
        building_id = building_id_scope

    title = (request.form.get("title") or "").strip()
    body = (request.form.get("body") or "").strip()
    target_group = (request.form.get("target_group") or "all").strip()

    if not building_id or not title or not body:
        flash("חסרים פרטים", "danger")
        return redirect(url_for("admin_announcements", building_id=building_id_scope))

    create_announcement_db(building_id, title, body, target_group)

    chat_ids = get_recipients_chat_ids_by_group_db(building_id, target_group)
    text = f"📢 {title}\n\n{body}"

    sent = 0
    for cid in chat_ids:
        send_telegram_message(cid, text)
        sent += 1

    flash(f"ההודעה נשלחה ({sent} נמענים).", "success")
    return redirect(url_for("admin_announcements", building_id=building_id))

# POLLS 
@app.get("/admin/polls")
@admin_required
def admin_polls():
    staff, role, building_id_scope = get_staff_scope()
    buildings = get_buildings_db() if role == "super_admin" else []
    polls = list_polls_db(building_id_scope, status=None)
    return render_template("admin_polls.html", staff=staff, role=role, buildings=buildings, building_id=building_id_scope, polls=polls)

@app.post("/admin/polls/create")
@admin_required
def admin_polls_create():
    staff, role, building_id_scope = get_staff_scope()

    building_id = request.form.get("building_id", type=int)
    if role == "building_admin":
        building_id = building_id_scope

    title = request.form.get("title") or ""
    description = request.form.get("description") or ""
    target_group = request.form.get("target_group") or "all"
    is_anonymous = request.form.get("is_anonymous", "1")
    closes_at = request.form.get("closes_at") or ""

    # options from textarea: each line option
    raw_opts = (request.form.get("options") or "").splitlines()
    options = [o.strip() for o in raw_opts if o.strip()]

    if not building_id:
        flash("חסר building_id", "danger")
        return redirect(url_for("admin_polls", building_id=building_id_scope))

    res = create_poll_db(building_id, title, description, target_group, int(is_anonymous), closes_at, options)
    if not res.get("ok"):
        flash("יש להזין לפחות 2 אפשרויות הצבעה", "danger")
        return redirect(url_for("admin_polls", building_id=building_id))

    flash(f"הצבעה נוצרה (#{res['poll_id']}).", "success")
    return redirect(url_for("admin_polls", building_id=building_id))

@app.post("/admin/polls/<int:poll_id>/send")
@admin_required
def admin_polls_send(poll_id: int):
    staff, role, building_id_scope = get_staff_scope()

    poll = get_poll_with_options_db(poll_id)
    if not poll:
        flash("הצבעה לא נמצאה", "danger")
        return redirect(url_for("admin_polls", building_id=building_id_scope))

    if role == "building_admin" and int(poll["building_id"]) != int(building_id_scope):
        abort(403)

    # ✅ אם כבר נשלח פעם אחת – לא שולחים שוב, עוברים לתוצאות
    if poll.get("sent_at"):
        flash("ההצבעה כבר נשלחה. מציג תוצאות.", "info")
        return redirect(url_for("admin_poll_results", poll_id=poll_id))

    building_id = int(poll["building_id"])
    chat_ids = get_recipients_chat_ids_by_group_db(building_id, poll["target_group"])

    text = f"🗳️ הצבעה חדשה:\n{poll['title']}\n\n{poll.get('description') or ''}\n\nבחר/י אפשרות:"
    buttons = [[{"text": opt["text"], "callback_data": f"poll_{poll_id}_{opt['id']}"}] for opt in poll["options"]]

    for cid in chat_ids:
        send_telegram_message(cid, text, buttons=buttons)

    # מסמן שנשלח
    mark_poll_sent_db(poll_id)

    flash(f"נשלח לדיירים ({len(chat_ids)}).", "success")
    return redirect(url_for("admin_poll_results", poll_id=poll_id))


@app.post("/api/polls/vote")
def api_polls_vote():
    data = request.get_json(force=True) or {}
    chat_id = int(data.get("chat_id") or 0)
    poll_id = int(data.get("poll_id") or 0)
    option_id = int(data.get("option_id") or 0)

    tenant = get_tenant_by_chat_id_db(chat_id)
    if not tenant:
        return jsonify({"ok": False, "error": "not_registered"}), 403

    poll = get_poll_with_options_db(poll_id)
    if not poll:
        return jsonify({"ok": False, "error": "poll_not_found"}), 404

    if int(poll["building_id"]) != int(tenant["building_id"]):
        return jsonify({"ok": False, "error": "forbidden"}), 403

    res = cast_vote_db(poll_id, option_id, int(tenant["id"]))
    return jsonify(res), (200 if res.get("ok") else 400)
    
@app.get("/admin/polls/<int:poll_id>/results")
@admin_required
def admin_poll_results(poll_id: int):
    staff, role, building_id_scope = get_staff_scope()

    poll = get_poll_with_options_db(poll_id)
    if not poll:
        flash("הצבעה לא נמצאה", "danger")
        return redirect(url_for("admin_polls", building_id=building_id_scope))

    if role == "building_admin" and int(poll["building_id"]) != int(building_id_scope):
        abort(403)

    results = poll_results_db(poll_id)
    return render_template("admin_poll_results.html", poll=poll, results=results)

#---Portal---#


# ---- Tenant session helpers ----
def tenant_login_required(view):
    from functools import wraps
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("tenant_id"):
            return redirect(url_for("tenant_login_info"))
        return view(*args, **kwargs)
    return wrapped


@app.get("/tenant")
def tenant_login_info():
    return render_template("tenant_login_info.html")


@app.get("/tenant/login")
def tenant_login():
    token = (request.args.get("token") or "").strip()
    print("TENANT_LOGIN HIT token=", token)

    if not token:
        print("TENANT_LOGIN: no token -> /tenant")
        return redirect("/tenant")

    rec = get_tenant_portal_token_db(token)
    print("TENANT_LOGIN rec=", rec)

    if not rec:
        print("TENANT_LOGIN: rec not found -> /tenant")
        return redirect("/tenant")

    if is_token_expired(rec["expires_at"]):
        print("TENANT_LOGIN: expired -> /tenant")
        return redirect("/tenant")

    session["tenant_id"] = int(rec["tenant_id"])
    mark_tenant_portal_token_used_db(int(rec["id"]))
    print("TENANT_LOGIN: session tenant_id set =", session.get("tenant_id"))

    # ✅ force dashboard
    return redirect("/tenant/dashboard")


@app.get("/tenant/logout")
def tenant_logout():
    session.pop("tenant_id", None)
    return redirect(url_for("tenant_login_info"))


@app.get("/tenant/dashboard")
@tenant_login_required
def tenant_dashboard():
    tenant_id = int(session["tenant_id"])
    tenant = get_tenant_by_id_db(tenant_id)
    if not tenant:
        session.pop("tenant_id", None)
        return redirect(url_for("tenant_login_info"))

    announcements = list_building_announcements_db(int(tenant.get("building_id") or 0), limit=5)
    tickets = list_tenant_tickets_db(int(tenant.get("chat_id") or 0), limit=20)
    payments = list_tenant_payments_db(tenant_id, limit=20)

    return render_template(
        "tenant_dashboard.html",
        tenant=tenant,
        announcements=announcements,
        tickets=tickets,
        payments=payments,
    )


# ---- API: bot asks for portal link ----
@app.post("/api/tenant_portal/create_link")
def api_tenant_portal_create_link():
    data = request.get_json(force=True) or {}
    chat_id = int(data.get("chat_id") or 0)
    tenant = get_tenant_by_chat_id_db(chat_id)

    if not tenant or not is_tenant_fully_registered(tenant):
        return jsonify({"ok": False, "error": "not_fully_registered"}), 403

    rec = create_tenant_portal_token_db(int(tenant["id"]), ttl_minutes=30)

    # חשוב: BASE_URL ציבורי (Railway) כדי שהלינק יעבוד מחוץ ללוקאל
    base_url = (request.host_url or "").rstrip("/")
    url = f"{base_url}/tenant/login?token={rec['token']}"

    return jsonify({"ok": True, "url": url, "expires_at": rec["expires_at"]})



if __name__ == "__main__":
    port = int(os.getenv("PORT", "5001"))
    app.run(host="0.0.0.0", port=port)    