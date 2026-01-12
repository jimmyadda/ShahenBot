import asyncio
import time
import logging
import os
import json
from pathlib import Path
import io
from dotenv import load_dotenv
import httpx
import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ───────────── Load .env ─────────────
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not found in .env file")

API_BASE_URL = os.getenv("SHAHEN_API_URL", "http://localhost:5001")
DISABLE_POLLING = os.getenv("DISABLE_POLLING", "").lower() == "true"

# ───────────── Logging ─────────────
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ───────────── Messages from JSON ─────────────
MESSAGES = {}


def load_messages():
    global MESSAGES
    path = Path(__file__).with_name("messages.json")
    if not path.exists():
        raise FileNotFoundError(f"messages.json not found at {path}")
    with path.open(encoding="utf-8") as f:
        MESSAGES = json.load(f)

def get_text(lang: str, key: str) -> str:
    if not MESSAGES:
        load_messages()
    data = MESSAGES.get(lang)
    if data is None:
        data = MESSAGES.get("he", {})
    return data.get(key, key)

def build_main_menu_keyboard(chat_id: int, lang: str):
    tenant = api_get_tenant_by_chat_id(chat_id)
    has_tenant = tenant is not None

    lang_row = [
        InlineKeyboardButton(get_text(lang, "lang_button_he"), callback_data="lang_he"),
        InlineKeyboardButton(get_text(lang, "lang_button_en"), callback_data="lang_en"),
        InlineKeyboardButton(get_text(lang, "lang_button_fr"), callback_data="lang_fr"),
    ]

    actions_row = [
        InlineKeyboardButton(get_text(lang, "btn_report"), callback_data="report"),
    ]

    rows = [lang_row, actions_row]

    # פורטל דיירים
    if tenant and int(tenant.get("building_id") or 0) > 0:
        name_ok = (tenant.get("name") or "").strip() and not (tenant.get("name") or "").startswith("New Tenant")
        apt_ok = (tenant.get("apartment") or "").strip()
        if name_ok and apt_ok:
            rows.append([
                InlineKeyboardButton(get_text(lang, "portal_open_btn"), callback_data="portal_open")
            ])

    # Register button 
    if not has_tenant:
        rows.append([
            InlineKeyboardButton(get_text(lang, "btn_register"), callback_data="register")
        ])

    return InlineKeyboardMarkup(rows)

# ───────────── API helpers ─────────────

def api_get_user_language(chat_id: int, default_lang: str = "he") -> str:
    try:
        url = f"{API_BASE_URL}/api/user/{chat_id}/language"
        resp = requests.get(url, timeout=5)
        if resp.ok:
            data = resp.json()
            return data.get("language", default_lang)
        else:
            logger.error("API get_language error: %s %s", resp.status_code, resp.text)
    except Exception as e:
        logger.exception("API get_language exception: %s", e)
    return default_lang

def api_set_user_language(chat_id: int, lang: str):
    try:
        url = f"{API_BASE_URL}/api/user/{chat_id}/language"
        resp = requests.post(url, json={"language": lang}, timeout=5)
        if not resp.ok:
            logger.error("API set_language error: %s %s", resp.status_code, resp.text)
    except Exception as e:
        logger.exception("API set_language exception: %s", e)

def api_create_ticket(chat_id: int, lang: str, category: str, description: str, image_url: str | None = None):
    try:
        url = f"{API_BASE_URL}/api/tickets"
        payload = {
            "chat_id": chat_id,
            "category": category,
            "description": description,
            "language": lang,
            "image_url": image_url,
        }
        resp = requests.post(url, json=payload, timeout=5)
        if resp.ok:
            return resp.json()
        else:
            logger.error("API create_ticket error: %s %s", resp.status_code, resp.text)
    except Exception as e:
        logger.exception("API create_ticket exception: %s", e)
    return None

def api_update_ticket_description(ticket_id: int, chat_id: int, new_description: str):
    """
    Update ticket description via API.
    Returns a dict like:
      { "success": True, "ticket": {...} }
      or
      { "success": False, "error": "ticket_closed" }
    """
    try:
        url = f"{API_BASE_URL}/api/tickets/{ticket_id}/description"
        payload = {"chat_id": chat_id, "description": new_description}
        resp = requests.post(url, json=payload, timeout=5)

        if resp.ok:
            return {"success": True, "ticket": resp.json()}

        # try to understand error body
        try:
            data = resp.json()
        except Exception:
            data = {}

        err = data.get("error")
        if err == "ticket_closed":
            return {"success": False, "error": "ticket_closed"}

        return {"success": False, "error": err or "unknown"}

    except Exception as e:
        logger.exception("API update_ticket_description exception: %s", e)
        return {"success": False, "error": "exception"}

def api_get_tenants_by_apartment(apartment: str, only_without_chat: bool = True):
    try:
        url = f"{API_BASE_URL}/api/tenants/by_apartment/{apartment}"
        params = {"only_without_chat": "1"} if only_without_chat else {}
        resp = requests.get(url, params=params, timeout=5)
        if resp.ok:
            data = resp.json()
            return data.get("tenants", [])
    except Exception as e:
        logger.exception("API get_tenants_by_apartment exception: %s", e)
    return []

def api_link_tenant_chat(tenant_id: int, chat_id: int):
    try:
        url = f"{API_BASE_URL}/api/tenants/{tenant_id}/link_chat"
        resp = requests.post(url, json={"chat_id": chat_id}, timeout=5)
        if resp.ok:
            return resp.json()
        else:
            logger.error(
                "API link_tenant_chat error: %s %s",
                resp.status_code,
                resp.text,
            )
    except Exception as e:
        logger.exception("API link_tenant_chat exception: %s", e)
    return None

def api_check_duplicate(building_id: int, category: str):
    r = httpx.get(
        f"{API_BASE_URL}/api/tickets/check_duplicate",
        params={"building_id": building_id, "category": category},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()

def api_add_ticket_watcher(ticket_id: int, chat_id: int):
    try:
        url = f"{API_BASE_URL}/api/tickets/{ticket_id}/watchers"
        resp = requests.post(url, json={"chat_id": chat_id}, timeout=5)
        if resp.ok:
            return {"success": True}
        try:
            data = resp.json()
        except Exception:
            data = {}
        return {"success": False, "error": data.get("error", "unknown")}
    except Exception as e:
        logger.exception("API add_ticket_watcher exception: %s", e)
        return {"success": False, "error": "exception"}

def api_get_tenant_by_chat_id(chat_id: int):
    r = httpx.get(f"{API_BASE_URL}/api/tenants/by_chat/{chat_id}", timeout=10)
    r.raise_for_status()
    return (r.json() or {}).get("tenant")

def api_get_my_tickets(chat_id: int):
    try:
        url = f"{API_BASE_URL}/api/tickets/by_chat/{chat_id}"
        resp = requests.get(url, timeout=8)
        if resp.ok:
            return resp.json()
    except Exception as e:
        logger.exception("api_get_my_tickets error: %s", e)
    return {"own": [], "watching": []}

def api_resolve_building(street: str, number: str):
    r = httpx.post(
        f"{API_BASE_URL}/api/buildings/resolve",
        json={"street": street, "number": number},
        timeout=10,
    )
    if r.status_code != 200:
        return None
    return r.json()

def api_get_tenants_by_building_apartment(building_id: int, apartment: str, only_without_chat: bool = True):
    r = httpx.get(
        f"{API_BASE_URL}/api/tenants/by_building_apartment",
        params={
            "building_id": building_id,
            "apartment": apartment,
            "only_without_chat": "1" if only_without_chat else "0",
        },
        timeout=10,
    )
    r.raise_for_status()
    return (r.json() or {}).get("tenants", [])

def api_create_tenant_auto(building_id: int, apartment: str, chat_id: int, language: str):
    r = httpx.post(
        f"{API_BASE_URL}/api/tenants/auto_register",
        json={
            "building_id": building_id,
            "apartment": apartment,
            "chat_id": chat_id,
            "language": language,
        },
        timeout=10,
    )
    if r.status_code != 200:
        return None
    return (r.json() or {}).get("tenant")

def api_update_tenant_name(tenant_id: int, name: str) -> bool:
    r = httpx.post(
        f"{API_BASE_URL}/api/tenants/{tenant_id}/name",
        json={"name": name},
        timeout=10,
    )
    return r.status_code == 200

async def handle_poll_vote(update: Update, context: ContextTypes.DEFAULT_TYPE, poll_id: int, option_id: int):
    query = update.callback_query
    await query.answer()

    chat_id = query.message.chat_id
    lang = api_get_user_language(chat_id)

    try:
        resp = requests.post(
            f"{API_BASE_URL}/api/polls/vote",
            json={"chat_id": chat_id, "poll_id": poll_id, "option_id": option_id},
            timeout=10,
        )

        if not resp.ok:
            data = {}
            try:
                data = resp.json() or {}
            except Exception:
                pass

            err = data.get("error")
            if err == "already_voted":
                await query.message.reply_text(get_text(lang, "poll_already_voted"))
            elif err == "poll_closed":
                await query.message.reply_text(get_text(lang, "poll_closed"))
            elif err == "not_registered":
                await query.message.reply_text(get_text(lang, "must_register_first"))
            else:
                await query.message.reply_text(get_text(lang, "poll_vote_failed"))
            return

        # הצלחה: אפשר גם לערוך את ההודעה המקורית כדי "לנעול" את הכפתורים
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        await query.message.reply_text(get_text(lang, "poll_vote_success"))

    except Exception:
        await query.message.reply_text(get_text(lang, "poll_vote_failed"))

def api_create_portal_link(chat_id: int):
    r = requests.post(
        f"{API_BASE_URL}/api/tenant_portal/create_link",
        json={"chat_id": chat_id},
        timeout=10,
    )
    if not r.ok:
        try:
            return {"ok": False, **(r.json() or {})}
        except Exception:
            return {"ok": False, "error": "http_error"}
    return r.json() or {}

# ───────────── Keyword-based category detection ─────────────

def detect_category_from_text(text: str, lang: str):
    low = (text or "").lower()

    # Elevator
    elevator_keywords = [
        "מעלית", "תקועה", "נתקעה", "לא עובדת", "תקלה",
        "elevator", "lift", "stuck"
    ]
    if any(k in low for k in elevator_keywords):
        return "cat_elevator", get_text(lang, "cat_elevator")

    # Noise
    noise_keywords = ["רעש", "מוזיקה", "צעק", "רועש", "noise", "loud"]
    if any(k in low for k in noise_keywords):
        return "cat_noise", get_text(lang, "cat_noise")

    # Parking
    parking_keywords = ["חניה", "חנייה", "חניון", "רכב", "parking", "park"]
    if any(k in low for k in parking_keywords):
        return "cat_parking", get_text(lang, "cat_parking")

    # Water / sewage
    water_keywords = [
        "מים", "ביוב", "נזילה", "רטיבות", "צינור", "הצפה",
        "water", "sewage", "leak", "flood"
    ]
    if any(k in low for k in water_keywords):
        return "cat_water", get_text(lang, "cat_water")

    return None, None

def parse_amount_to_cents(s: str):
    s = (s or "").strip()
    if not s:
        return None

    s = s.replace("₪", "").replace("ils", "").replace("ILS", "").strip()
    s = s.replace(",", ".")
    cleaned = "".join(ch for ch in s if ch.isdigit() or ch == ".")
    if cleaned.count(".") > 1 or cleaned == "":
        return None

    try:
        val = float(cleaned)
    except Exception:
        return None

    if val <= 0 or val > 100000:
        return None

    return int(round(val * 100))
# ───────────── Telegram Handlers ─────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    lang = api_get_user_language(chat_id)

    keyboard = build_main_menu_keyboard(chat_id, lang)

    await update.message.reply_text(
        get_text(lang, "start"),
        reply_markup=keyboard,
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = query.message.chat.id

    lang = api_get_user_language(chat_id)

    # Language change
    if data.startswith("lang_"):
        if data == "lang_he":
            api_set_user_language(chat_id, "he")
            lang = "he"
            text = get_text(lang, "language_set")
        elif data == "lang_en":
            api_set_user_language(chat_id, "en")
            lang = "en"
            text = get_text(lang, "language_set_en")
        elif data == "lang_fr":
            api_set_user_language(chat_id, "fr")
            lang = "fr"
            text = get_text(lang, "language_set_fr")

        keyboard = build_main_menu_keyboard(chat_id, lang)

        await query.edit_message_text(
            text=f"{text}\n\n{get_text(lang, 'main_menu')}",
            reply_markup=keyboard,
        )
        return

    # Manual "report" flow
    if data == "report":
        keyboard = [
            [InlineKeyboardButton(get_text(lang, "cat_parking"), callback_data="parking")],
            [InlineKeyboardButton(get_text(lang, "cat_noise"), callback_data="noise")],
            [InlineKeyboardButton(get_text(lang, "cat_water"), callback_data="water")],
            [InlineKeyboardButton(get_text(lang, "cat_elevator"), callback_data="elevator")],
            [InlineKeyboardButton(get_text(lang, "cat_other"), callback_data="other")],
        ]
        await query.edit_message_text(
            get_text(lang, "choose_category"),
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return
    #Payments
    # inside button_handler
    if data == "pay_open":
        await handle_pay_open(update, context)
        return

    if data in ("pay_method_bank", "pay_method_bit"):
        await handle_pay_method(update, context)
        return
    # Manual category picked
    if data in ["parking", "noise", "water", "elevator", "other"]:
        key_map = {
            "parking": "cat_parking",
            "noise": "cat_noise",
            "water": "cat_water",
            "elevator": "cat_elevator",
            "other": "cat_other",
        }
        cat_label = get_text(lang, key_map[data])
        context.user_data["category"] = cat_label

        await query.edit_message_text(
            f"{cat_label}\n\n{get_text(lang, 'describe_problem')}"
        )
        return

    # Confirm ticket from auto-detect
    if data == "confirm_yes":
        pending = context.user_data.get("pending_ticket")
        if not pending:
            await query.edit_message_text("No pending ticket found.")
            return

        category = pending["category"]
        description = pending["description"]
        pending_lang = pending.get("lang", lang)
        image_url = pending.get("image_url")

        ticket = api_create_ticket(
            chat_id=chat_id,
            lang=pending_lang,
            category=category,
            description=description,
            image_url=image_url,
        )

        base_reply = get_text(pending_lang, "thanks").format(
            category=category,
            desc=description,
        )

        if ticket and "id" in ticket:
            ticket_id = ticket["id"]
            reply_text = f"{base_reply}\nID: #{ticket_id}"
            keyboard = [
                [InlineKeyboardButton(get_text(lang, "edit_ticket_button"), callback_data=f"edit_{ticket_id}")]
            ]
            await query.edit_message_text(
                reply_text,
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        else:
            await query.edit_message_text(base_reply)

        context.user_data.pop("pending_ticket", None)
        return

    if data == "confirm_no":
        context.user_data.pop("pending_ticket", None)
        await query.edit_message_text(get_text(lang, "cancelled"))
        return
    
    if data in ("register", "go_register"):
        tenant = api_get_tenant_by_chat_id(chat_id)

        is_registered = bool(
            tenant
            and int(tenant.get("building_id") or 0) > 0
            and (tenant.get("apartment") or "").strip()
        )

        if is_registered:
            txt = get_text(lang, "register_already_linked").format(
                name=tenant.get("name", ""),
                apartment=tenant.get("apartment") or "",
            )
            await query.edit_message_text(txt)
            return
        
        logger.info(tenant,is_registered)


        # start full register flow (street -> building -> apartment)
        context.user_data.clear()
        context.user_data["register_step"] = "street"
        await query.edit_message_text(get_text(lang, "register_ask_street"))
        return

        # Duplicate ticket: add watcher?
    
    if data == "dup_yes":
        dup_ticket_id = context.user_data.get("dup_ticket_id")
        lang = api_get_user_language(chat_id)

        if dup_ticket_id:
            result = api_add_ticket_watcher(dup_ticket_id, chat_id)
            if result.get("success"):
                await query.edit_message_text(
                    get_text(lang, "dup_added_watcher").format(ticket_id=dup_ticket_id)
                )
            else:
                err = result.get("error")
                if err == "not_registered":
                    keyboard = InlineKeyboardMarkup(
                        [
                            [
                                InlineKeyboardButton(
                                    get_text(lang, "btn_register"),
                                    callback_data="register",
                                )
                            ]
                        ]
                    )

                    await query.edit_message_text(
                        get_text(lang, "dup_need_register"),
                        reply_markup=keyboard,
                    )
                else:
                    await query.edit_message_text(get_text(lang, "dup_add_watcher_fail"))
        else:
            await query.edit_message_text(get_text(lang, "dup_no_context"))

        context.user_data.pop("dup_ticket_id", None)
        return

    if data == "dup_no":
        lang = api_get_user_language(chat_id)
        await query.edit_message_text(get_text(lang, "dup_declined"))
        context.user_data.pop("dup_ticket_id", None)
        return
    
    if data == "portal_open":
        res = api_create_portal_link(chat_id)
        if not res.get("ok"):
            await query.message.reply_text(get_text(lang, "portal_need_register"))
            return

        url = res.get("url")
        # URL button (לא callback_data!)
        keyboard = [[InlineKeyboardButton(get_text(lang, "portal_open_btn"), url=url)]]
        await query.message.reply_text(get_text(lang, "portal_link_ready"), reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("poll_"):
        parts = data.split("_")
        if len(parts) == 3:
            poll_id = int(parts[1])
            option_id = int(parts[2])
            await handle_poll_vote(update, context, poll_id, option_id)
        return   
    # /register flow – user chooses tenant from list
    if data.startswith("regtenant_"):
        tenant_id = int(data.split("_")[1])
        chat_id = query.message.chat.id
        lang = api_get_user_language(chat_id)

        linked = api_link_tenant_chat(tenant_id, chat_id)
        if linked:
            text = get_text(lang, "register_success").format(
                name=linked.get("name", ""),
                apartment=linked.get("apartment") or "",
            )
        else:
            text = get_text(lang, "register_link_fail")

        await query.edit_message_text(text)
        return
    # Edit ticket button
    if data.startswith("edit_"):
        tid = int(data.split("_")[1])
        context.user_data["editing_ticket_id"] = tid
        context.user_data["awaiting_edit"] = True

        await query.message.reply_text(get_text(lang, "edit_ticket_prompt"))
        return

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    text = msg.text or ""
    chat = msg.chat
    chat_id = chat.id
    lang = api_get_user_language(chat_id)
    chat_type = chat.type
    #add name to auto register
    # MUST be first in text_handler
     # ✅ PAYMENT: awaiting amount MUST be first (before name/register/tickets)
    if context.user_data.get("payment_step") == "awaiting_amount":
        cents = parse_amount_to_cents(text)
        if cents is None:
            await msg.reply_text(get_text(lang, "payment_amount_invalid"))
            return

        method = context.user_data.get("payment_method") or "bank_transfer"

        try:
            resp = requests.post(
                f"{API_BASE_URL}/api/payments/create_pending",
                json={"chat_id": chat_id, "amount_cents": cents, "method": method},
                timeout=10,
            )
            logger.info("PAY: create_pending status=%s text=%s", resp.status_code, resp.text)

            if not resp.ok:
                await msg.reply_text(get_text(lang, "payment_error_try_again"))
                context.user_data.pop("payment_step", None)
                context.user_data.pop("payment_method", None)
                return

            data = resp.json() or {}
            payment_id = data.get("payment_id")
            logger.info("PAY: payment_id=%s", payment_id)

            if not payment_id:
                await msg.reply_text(get_text(lang, "payment_error_try_again"))
                context.user_data.pop("payment_step", None)
                context.user_data.pop("payment_method", None)
                return

            context.user_data["payment_step"] = "awaiting_proof"
            context.user_data["payment_id"] = payment_id

            await msg.reply_text(get_text(lang, "payment_amount_received_send_proof"))
            return

        except Exception as e:
            logger.exception("PAY: create_pending exception: %s", e)
            await msg.reply_text(get_text(lang, "payment_error_try_again"))
            context.user_data.pop("payment_step", None)
            context.user_data.pop("payment_method", None)
            return
        
    if context.user_data.get("awaiting_name"):
        tenant_id = context.user_data.get("name_tenant_id")
        name = text.strip()

        ok = api_update_tenant_name(tenant_id, name)
        if ok:
            await msg.reply_text(get_text(lang, "register_name_saved").format(name=name))
        else:
            await msg.reply_text(get_text(lang, "register_name_save_failed"))

        context.user_data.clear()
        return   # ✅ REQUIRED

    tenant = api_get_tenant_by_chat_id(chat_id)

    if tenant and int(tenant.get("building_id") or 0) > 0:
        # registered
        if not (tenant.get("name") or "").strip() or (tenant.get("name") or "").startswith("New Tenant"):
            # ask name only if missing/placeholder
            context.user_data["awaiting_name"] = True
            context.user_data["name_tenant_id"] = tenant["id"]
            await msg.reply_text(get_text(lang, "register_success_ask_name"))
            return
    else:
        # not registered -> allow tickets OR force register, your choice
        pass
    # Editing ticket flow
    if context.user_data.get("awaiting_edit"):
        ticket_id = context.user_data.get("editing_ticket_id")
        new_text = text

        result = api_update_ticket_description(ticket_id, chat_id, new_text)

        if result and result.get("success"):
            await msg.reply_text(
                f"{get_text(lang, 'edit_ticket_success')}\nID: #{ticket_id}\n\n{new_text}"
            )
        else:
            # check specific error
            err = (result or {}).get("error")
            if err == "ticket_closed":
                await msg.reply_text(get_text(lang, "edit_ticket_closed"))
            else:
                await msg.reply_text(get_text(lang, "edit_ticket_fail"))

        context.user_data.pop("awaiting_edit", None)
        context.user_data.pop("editing_ticket_id", None)
        return

    # -------- Registration flow (street -> building -> apartment) --------
    step = context.user_data.get("register_step")
    
    if step == "street":
        context.user_data["street"] = text.strip()
        context.user_data["register_step"] = "building_number"
        await msg.reply_text(get_text(lang, "register_ask_building_number"))
        return

    if step == "building_number":
        context.user_data["building_number"] = text.strip()
        context.user_data["register_step"] = "apartment"
        await msg.reply_text(get_text(lang, "register_ask_apartment"))
        return

    if step == "apartment":
        street = context.user_data.get("street", "").strip()
        number = context.user_data.get("building_number", "").strip()
        apartment = text.strip()

        # Resolve building first (must exist or be created by admin/superadmin)
        building = api_resolve_building(street=street, number=number)
        if not building:
            await msg.reply_text(get_text(lang, "register_building_not_found").format(street=street, number=number))
            context.user_data.clear()
            return

        building_id = int(building["id"])
        logger.info(building_id,apartment)
        
        tenants = api_get_tenants_by_building_apartment(building_id, apartment, only_without_chat=True)

        if not tenants:
            # Auto create tenant and link chat_id
            created = api_create_tenant_auto(
                building_id=building_id,
                apartment=apartment,
                chat_id=chat_id,
                language=lang,
            )
            if created:
                tenant_id = created["id"]
                context.user_data["awaiting_name"] = True
                context.user_data["name_tenant_id"] = tenant_id

                await msg.reply_text(get_text(lang, "register_success_ask_name"))
                return
            else:
                await msg.reply_text(get_text(lang, "register_failed"))

            context.user_data.clear()
            return

        if len(tenants) == 1:
            t = tenants[0]
            linked = api_link_tenant_chat(t["id"], chat_id)
            if linked:
                await msg.reply_text(
                    get_text(lang, "register_success").format(
                        name=linked.get("name", ""),
                        apartment=linked.get("apartment") or apartment,
                    )
                )
            else:
                await msg.reply_text(get_text(lang, "register_link_fail"))

            context.user_data.clear()
            return

        # More than one tenant (husband/wife/owner) -> choose
        context.user_data["register_step"] = None
        context.user_data["reg_building_id"] = building_id
        context.user_data["reg_apartment"] = apartment

        buttons = []
        for t in tenants:
            label_parts = [t.get("name", "")]
            if t.get("tenant_type") == "owner":
                label_parts.append("(" + get_text(lang, "tenant_type_owner_short") + ")")
            elif t.get("tenant_type") == "rent":
                label_parts.append("(" + get_text(lang, "tenant_type_rent_short") + ")")
            label = " ".join(p for p in label_parts if p)

            buttons.append([InlineKeyboardButton(label, callback_data=f"regtenant_{t['id']}")])

        await msg.reply_text(
            get_text(lang, "register_choose_tenant").format(apartment=apartment),
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return
    # -------- End registration flow --------


    # Manual flow: user chose category via buttons, now sends description
    if "category" in context.user_data:
        category = context.user_data["category"]

        logger.info(
            "Manual report from chat %s: [%s] %s",
            chat_id,
            category,
            text,
        )

        ticket = api_create_ticket(
            chat_id=chat_id,
            lang=lang,
            category=category,
            description=text,
        )

        base_reply = get_text(lang, "thanks").format(
            category=category,
            desc=text,
        )

        if ticket and "id" in ticket:
            ticket_id = ticket["id"]
            reply_text = f"{base_reply}\nID: #{ticket_id}"
            keyboard = [
                [InlineKeyboardButton(get_text(lang, "edit_ticket_button"), callback_data=f"edit_{ticket_id}")]
            ]
            await msg.reply_text(
                reply_text,
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        else:
            await msg.reply_text(base_reply)

        context.user_data.clear()
        return

    # Auto-detect category from free text
    cat_key, cat_label = detect_category_from_text(text, lang)

    if cat_key is not None and cat_label is not None:
     # ✅ Must be registered to do duplicate/watch logic
        tenant = api_get_tenant_by_chat_id(chat_id)  # returns {id, building_id, name, apartment...} or None     
        if not tenant or int(tenant.get("building_id") or 0) <= 0:
            # Not registered -> do NOT check duplicates / do NOT create ticket
            text_need_reg = get_text(lang, "must_register_first")
            keyboard = [[InlineKeyboardButton(get_text(lang, "btn_register"), callback_data="go_register")]]
            await msg.reply_text(text_need_reg, reply_markup=InlineKeyboardMarkup(keyboard))
            return

        building_id = int(tenant["building_id"])
        # First check duplicate
        dup_info = api_check_duplicate(building_id, cat_label)
        if dup_info.get("duplicate") and dup_info.get("ticket"):
            t = dup_info["ticket"]
            dup_id = t["id"]
            dup_desc = t.get("description", "")

            context.user_data["dup_ticket_id"] = dup_id

            text_dup = get_text(lang, "dup_ticket_found").format(
                category=cat_label,
                ticket_id=dup_id,
                desc=dup_desc,
            )

            keyboard = [
                [
                    InlineKeyboardButton(get_text(lang, "btn_yes"), callback_data="dup_yes"),
                    InlineKeyboardButton(get_text(lang, "btn_no"), callback_data="dup_no"),
                ]
            ]

            await msg.reply_text(
                text_dup,
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        # No duplicate – normal pending ticket flow
        context.user_data["pending_ticket"] = {
            "category": cat_label,
            "description": text,
            "lang": lang,
        }

        confirm_text = get_text(lang, "auto_detect_proposed").format(
            category=cat_label,
            desc=text,
        )

        keyboard_rows = [
            [
                InlineKeyboardButton(get_text(lang, "btn_yes"), callback_data="confirm_yes"),
                InlineKeyboardButton(get_text(lang, "btn_no"), callback_data="confirm_no"),
            ]
        ]

        tenant = api_get_tenant_by_chat_id(chat_id)
        if not tenant:
            keyboard_rows.append(
                [
                    InlineKeyboardButton(
                        get_text(lang, "btn_register"),
                        callback_data="register",
                    )
                ]
            )

        await msg.reply_text(
            confirm_text,
            reply_markup=InlineKeyboardMarkup(keyboard_rows),
        )
        return


    # In private chat, show main menu
    keyboard = build_main_menu_keyboard(chat_id, lang)
    await msg.reply_text(
        get_text(lang, "main_menu"),
        reply_markup=keyboard,
    )

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Error while handling update:", exc_info=context.error)

async def register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat_id = update.effective_chat.id

    lang = api_get_user_language(chat_id)  # or your existing language getter

    tenant = api_get_tenant_by_chat_id(chat_id)
    is_registered = bool(
        tenant
        and int(tenant.get("building_id") or 0) > 0
        and (tenant.get("apartment") or "").strip()
    )

    if is_registered:
        txt = get_text(lang, "register_already_linked").format(
            name=tenant.get("name", ""),
            apartment=tenant.get("apartment") or "",
        )
        await msg.reply_text(txt)
        return

    # ✅ start NEW flow (street -> building -> apartment)
    context.user_data.clear()
    context.user_data["register_step"] = "street"

    await msg.reply_text(get_text(lang, "register_ask_street"))

async def photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    chat_id = msg.chat_id
    lang = api_get_user_language(chat_id)

    caption = msg.caption or ""
    if not caption.strip():
        await msg.reply_text(get_text(lang, "photo_need_caption"))
        return
    if context.user_data.get("payment_step") == "awaiting_proof":
       return
    # Get best resolution photo
    photo = msg.photo[-1]
    tg_file = await photo.get_file()

    bio = io.BytesIO()
    await tg_file.download_to_memory(out=bio)
    bio.seek(0)

    files = {"file": ("report.jpg", bio, "image/jpeg")}

    try:
        resp = requests.post(f"{API_BASE_URL}/api/upload_image", files=files, timeout=15)
        if not resp.ok:
            logger.error("Upload image error: %s %s", resp.status_code, resp.text)
            await msg.reply_text(get_text(lang, "photo_upload_fail"))
            return
        data = resp.json()
        image_url = data.get("url")
    except Exception as e:
        logger.exception("Upload image exception: %s", e)
        await msg.reply_text(get_text(lang, "photo_upload_fail"))
        return

    # Detect category from caption
    cat_key, cat_label = detect_category_from_text(caption, lang)

    if cat_key is None or cat_label is None:
        # Ask user to choose category manually, but keep image_url and caption
        context.user_data["pending_photo"] = {
            "description": caption,
            "image_url": image_url,
            "lang": lang,
        }
        keyboard = [
            [InlineKeyboardButton(get_text(lang, "cat_parking"), callback_data="p_photo_parking")],
            [InlineKeyboardButton(get_text(lang, "cat_noise"), callback_data="p_photo_noise")],
            [InlineKeyboardButton(get_text(lang, "cat_water"), callback_data="p_photo_water")],
            [InlineKeyboardButton(get_text(lang, "cat_elevator"), callback_data="p_photo_elevator")],
            [InlineKeyboardButton(get_text(lang, "cat_other"), callback_data="p_photo_other")],
        ]
        await msg.reply_text(
            get_text(lang, "photo_choose_category"),
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    # Category detected – reuse text auto-detect flow, but with image_url stored
    context.user_data["pending_ticket"] = {
        "category": cat_label,
        "description": caption,
        "lang": lang,
        "image_url": image_url,
    }

    # BEFORE creating ticket – we'll later add duplicate detection here (section 4)
    confirm_text = get_text(lang, "auto_detect_proposed").format(
        category=cat_label,
        desc=caption,
    )

    keyboard_rows = [
        [
            InlineKeyboardButton(get_text(lang, "btn_yes"), callback_data="confirm_yes"),
            InlineKeyboardButton(get_text(lang, "btn_no"), callback_data="confirm_no"),
        ]
    ]

    tenant = api_get_tenant_by_chat_id(chat_id)
    if not tenant:
        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    get_text(lang, "btn_register"), callback_data="register"
                )
            ]
        )

    await msg.reply_text(
        confirm_text,
        reply_markup=InlineKeyboardMarkup(keyboard_rows),
    )

async def mytickets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    lang = api_get_user_language(chat_id)

    data = api_get_my_tickets(chat_id)
    own = data.get("own", [])
    watching = data.get("watching", [])

    if not own and not watching:
        await update.message.reply_text(get_text(lang, "mytickets_none"))
        return

    msg_parts = []

    if own:
        msg_parts.append(get_text(lang, "mytickets_yours"))
        for t in own:
            msg_parts.append(
                f"#{t['id']} – {t['category']} – {t['status']}\n"
                f"{t['description']}\n"
            )

    if watching:
        msg_parts.append(get_text(lang, "mytickets_watching"))
        for t in watching:
            msg_parts.append(
                f"#{t['id']} – {t['category']} – {t['status']}\n"
                f"{t['description']}\n"
            )

    await update.message.reply_text("\n".join(msg_parts))

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    lang = api_get_user_language(chat_id)

    text = (
        "ShahenBot – Available Commands:\n\n"
        "/start – Main menu\n"
        "/register – Link your apartment\n"
        "/tenantsportal – Open the Tenant Portal (magic link)\n"
        "/mytickets – Show your tickets\n"
        "/help – Show this help menu\n\n"
        "You can also:\n"
        "• Send a message describing a problem\n"
        "• Send a photo with a caption\n"
        "• Use the buttons to report noise / parking / water / elevator issues"
    )

    await update.message.reply_text(text, parse_mode="Markdown")

async def handle_pay_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    chat_id = query.message.chat_id
    lang = api_get_user_language(chat_id)

    # reset state for payment flow
    context.user_data.pop("pending_ticket", None)
    context.user_data.pop("pending_photo", None)

    context.user_data["payment_step"] = "choose_method"

    keyboard = [
        [InlineKeyboardButton(get_text(lang, "payment_method_bank"), callback_data="pay_method_bank")],
        [InlineKeyboardButton(get_text(lang, "payment_method_bit"), callback_data="pay_method_bit")]
    ]

    await query.edit_message_text(
        get_text(lang, "payment_choose_method"),
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

async def handle_pay_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    chat_id = query.message.chat_id
    lang = api_get_user_language(chat_id)

    method = "bank_transfer" if query.data == "pay_method_bank" else "bit"
    logger.info("PAY: method selected=%s chat_id=%s", method, chat_id)    
    

    # Create pending payment in Flask
    # After selecting payment method:
    context.user_data["payment_step"] = "awaiting_amount"
    context.user_data["payment_method"] = method  # bank_transfer / bit

    try:
        await query.edit_message_text(get_text(lang, "payment_enter_amount"))
    except Exception:
        await query.message.reply_text(get_text(lang, "payment_enter_amount"))

    return

async def payment_proof_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return

    # only in payment flow
    if context.user_data.get("payment_step") != "awaiting_proof":
        return

    chat_id = msg.chat_id
    lang = api_get_user_language(chat_id)

    payment_id = context.user_data.get("payment_id")
    if not payment_id:
        context.user_data.pop("payment_step", None)
        await msg.reply_text(get_text(lang, "payment_error_try_again"))
        return

    if msg.photo:
        file_id = msg.photo[-1].file_id
        file_type = "photo"
    elif msg.document:
        file_id = msg.document.file_id
        file_type = "document"
    else:
        await msg.reply_text(get_text(lang, "payment_send_proof_photo_or_file"))
        return

    try:
        resp = requests.post(
            f"{API_BASE_URL}/api/payments/{payment_id}/attach_proof",
            json={"file_id": file_id, "file_type": file_type},
            timeout=10,
        )
        if not resp.ok:
            logger.error("attach_proof error: %s %s", resp.status_code, resp.text)
            await msg.reply_text(get_text(lang, "payment_proof_upload_fail"))
            return
    except Exception as e:
        logger.exception("attach_proof exception: %s", e)
        await msg.reply_text(get_text(lang, "payment_proof_upload_fail"))
        return

    # clear payment state
    context.user_data.pop("payment_step", None)
    context.user_data.pop("payment_id", None)
    context.user_data.pop("payment_method", None)

    await msg.reply_text(get_text(lang, "payment_proof_received_pending_admin"))

async def tenants_portal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message  # ✅ works even if update.message is None
    chat_id = update.effective_chat.id
    lang = api_get_user_language(chat_id)

    tenant = api_get_tenant_by_chat_id(chat_id)

    # must be fully registered
    if not tenant or int(tenant.get("building_id") or 0) <= 0:
        await msg.reply_text(get_text(lang, "portal_need_register"))
        return

    name_ok = (tenant.get("name") or "").strip() and not (tenant.get("name") or "").startswith("New Tenant")
    apt_ok = (tenant.get("apartment") or "").strip()
    if not (name_ok and apt_ok):
        await msg.reply_text(get_text(lang, "portal_need_register"))
        return

    res = api_create_portal_link(chat_id)
    if not res or not res.get("ok") or not res.get("url"):
        await msg.reply_text(get_text(lang, "portal_error"))
        return

    url = res["url"]
    print('URL- ',url)
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(lang, "portal_open_btn"), url=url)]])

    await msg.reply_text(get_text(lang, "portal_link_ready"), reply_markup=kb)


def main():
    if DISABLE_POLLING:
        logging.warning("🚫 Telegram polling is DISABLED (DISABLE_POLLING=true)")
        # Keep process alive on Railway free plan
        while True:
            time.sleep(3600)

    load_messages()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("register", register))
    app.add_handler(CommandHandler("mytickets", mytickets))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_handler(MessageHandler((filters.PHOTO | filters.Document.ALL) & ~filters.COMMAND, payment_proof_handler))
    app.add_handler(MessageHandler(filters.PHOTO & ~filters.COMMAND, photo_handler))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("tenantsportal", tenants_portal_command))
    app.add_error_handler(error_handler)

    print(f"ShahenBot is running. API base: {API_BASE_URL}")
    app.run_polling()   # ✅ NO await


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()