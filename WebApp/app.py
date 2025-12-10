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
)


load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Initialize Flask app
app = Flask(__name__)

# Initialize DB tables on startup
init_db()

def send_telegram_message(chat_id: int, text: str):
    """
    Send a message to a Telegram user when ticket status changes, etc.
    """
    if not BOT_TOKEN:
        app.logger.warning("BOT_TOKEN not set, cannot send Telegram messages.")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=5)
        if not resp.ok:
            app.logger.error(
                "Failed to send Telegram message: %s %s",
                resp.status_code,
                resp.text,
            )
    except Exception as e:
        app.logger.exception("Error sending Telegram message: %s", e)



@app.route("/")
def home():
    return "ShahenBot WebApp API is running", 200

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
        status="open",
    )

    return jsonify(ticket), 201


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
def admin_update_status(ticket_id: int):
    new_status = request.form.get("status")
    if new_status not in ("open", "in_progress", "closed"):
        return redirect(url_for("admin_dashboard"))

    update_ticket_status_db(ticket_id, new_status)

    ticket = get_ticket_by_id_db(ticket_id)

    # Notify user via Telegram
    if ticket:
        chat_id = ticket["chat_id"]
        lang = ticket["language"]
        if lang == "he":
            if new_status == "open":
                s = "פתוח"
            elif new_status == "in_progress":
                s = "בטיפול"
            else:
                s = "נסגר"
            text = f"הדיווח #{ticket['id']} עודכן לסטטוס: {s}"
        else:
            text = f"Your ticket #{ticket['id']} status changed to {new_status}"

        send_telegram_message(chat_id, text)

    return redirect(url_for("admin_dashboard", **request.args))


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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)