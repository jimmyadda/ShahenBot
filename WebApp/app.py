# WebApp/app.py
import os
from flask import (
    Flask,
    jsonify,
    request,
    render_template,
    redirect,
    url_for,
)
from dotenv import load_dotenv
import requests
import uuid
from werkzeug.utils import secure_filename
from shahenbot_db import (
    init_db,
    get_user_language_db,
    set_user_language_db,
    create_ticket_db,
    get_tickets_db,
    update_ticket_status_db,
    update_ticket_description_db,
    get_ticket_by_id_db,
    get_tenants_db,
    create_tenant_db,
    update_tenant_db,
    get_tenants_by_apartment_db,    # NEW
    get_tenant_by_chat_id_db,       # already exists
    link_tenant_chat_db,
    find_open_ticket_by_category_db,
    add_ticket_watcher_db,              
    get_ticket_watchers_db,
    get_tickets_for_chat_db   
)


UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), "static", "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

# Initialize Flask app
app = Flask(__name__)

# Initialize DB tables on startup
init_db()

def send_telegram_message(chat_id: int, text: str):
    if not BOT_TOKEN:
        print("BOT_TOKEN not set, cannot send Telegram messages")
        return
    try:
        resp = requests.post(
            f"{TELEGRAM_API}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=10,
        )
        if not resp.ok:
            print("Telegram sendMessage error:", resp.status_code, resp.text)
    except Exception as e:
        print("Telegram sendMessage exception:", e)




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

@app.post("/api/tickets/check_duplicate")
def api_check_duplicate():
    data = request.get_json(silent=True) or {}
    category = data.get("category")

    if not category:
        return jsonify({"error": "missing_category"}), 400

    ticket = find_open_ticket_by_category_db(category)
    if not ticket:
        return jsonify({"duplicate": False})

    return jsonify({"duplicate": True, "ticket": ticket})

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
    status = request.args.get("status", "all")
    category = request.args.get("category", "all")
    search = request.args.get("search", "").strip()
    try:
        limit = int(request.args.get("limit", 50))
    except ValueError:
        limit = 50

    tickets = get_tickets_db(
        limit=limit,
        status=status,
        category=category,
        search=search,
    )
    status_options = [
        ("all", "All"),
        ("open", "Open"),
        ("in_progress", "In progress"),
        ("closed", "Closed"),
    ]

    category_options = [
        ("all", "All"),
        ("חניה", "חניה"),
        ("מעלית", "מעלית"),
        ("מים", "מים/ביוב"),
        ("רעש", "רעש"),
    ]

    return render_template(
        "admin.html",
        tickets=tickets,
        status=status,
        category=category,
        search=search,
        limit=limit,
        status_options=status_options,
        category_options=category_options,
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

        for cid in recipients:
            send_telegram_message(cid, notify_text)

    return redirect(url_for("admin_dashboard"))


# ───────────────────────────────────────────────
#   ADMIN: TENANTS LIST + EDIT
# ───────────────────────────────────────────────
@app.get("/admin/tenants")
def admin_tenants():
    search = request.args.get("search", "").strip()
    tenants = get_tenants_db(limit=300, search=search)
    return render_template(
        "tenants.html",
        tenants=tenants,
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
    chat_id = request.form.get("chat_id") or None

    parking_slots = int(parking_slots) if parking_slots else None
    chat_id = int(chat_id) if chat_id else None

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
        chat_id=chat_id,
    )

    return redirect(url_for("admin_tenants", **request.args))

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
    tenant = get_tenant_by_chat_id_db(chat_id)
    if not tenant:
        return jsonify({"error": "not_found"}), 404
    return jsonify(tenant)

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

""" if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True) """

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5001"))
    app.run(host="0.0.0.0", port=port)    