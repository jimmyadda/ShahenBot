import logging
import os
import json
from pathlib import Path
import io
from dotenv import load_dotenv
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
    """
    Build main inline keyboard: language row + actions row.
    Shows 'Register' button until user is linked to a tenant.
    """
    # Need this helper to check if user already linked to tenant
    tenant = api_get_tenant_by_chat(chat_id)
    has_tenant = tenant is not None

    lang_row = [
        InlineKeyboardButton(get_text(lang, "lang_button_he"), callback_data="lang_he"),
        InlineKeyboardButton(get_text(lang, "lang_button_en"), callback_data="lang_en"),
        InlineKeyboardButton(get_text(lang, "lang_button_fr"), callback_data="lang_fr"),
    ]

    actions_row = [
        InlineKeyboardButton(get_text(lang, "btn_report"), callback_data="report"),
    ]

    if not has_tenant:
        actions_row.append(
            InlineKeyboardButton(get_text(lang, "btn_register"), callback_data="register")
        )

    return InlineKeyboardMarkup([lang_row, actions_row])

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

def api_get_tenant_by_chat(chat_id: int):
    try:
        url = f"{API_BASE_URL}/api/tenants/by_chat/{chat_id}"
        resp = requests.get(url, timeout=5)
        if resp.ok:
            return resp.json()
    except Exception as e:
        logger.exception("API get_tenant_by_chat exception: %s", e)
    return None

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

def api_check_duplicate(category: str):
    try:
        url = f"{API_BASE_URL}/api/tickets/check_duplicate"
        resp = requests.post(url, json={"category": category}, timeout=5)
        if resp.ok:
            return resp.json()
    except Exception as e:
        logger.exception("API check_duplicate exception: %s", e)
    return {"duplicate": False}

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

def api_get_my_tickets(chat_id: int):
    try:
        url = f"{API_BASE_URL}/api/tickets/by_chat/{chat_id}"
        resp = requests.get(url, timeout=8)
        if resp.ok:
            return resp.json()
    except Exception as e:
        logger.exception("api_get_my_tickets error: %s", e)
    return {"own": [], "watching": []}
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
    
    # Register button in main menu
    if data == "register":
        # simulate /register behavior
        tenant = api_get_tenant_by_chat(chat_id)
        if tenant:
            txt = get_text(lang, "register_already_linked").format(
                name=tenant.get("name", ""),
                apartment=tenant.get("apartment") or "",
            )
            await query.edit_message_text(txt)
            return

        context.user_data["awaiting_apartment"] = True
        await query.edit_message_text(get_text(lang, "register_prompt"))
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
    

    # /register flow – user just sent apartment number/name
    if context.user_data.get("awaiting_apartment"):
        apartment = text.strip()

        tenants = api_get_tenants_by_apartment(apartment, only_without_chat=True)

        # no tenants found
        if not tenants:
            await msg.reply_text(
                get_text(lang, "register_not_found").format(apartment=apartment)
            )
            context.user_data.pop("awaiting_apartment", None)
            return

        # exactly one tenant – link directly
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

            context.user_data.pop("awaiting_apartment", None)
            return

        # more than one tenant – let user choose
        context.user_data.pop("awaiting_apartment", None)

        buttons = []
        for t in tenants:
            label_parts = [t.get("name", "")]
            if t.get("tenant_type") == "owner":
                label_parts.append("(" + get_text(lang, "tenant_type_owner_short") + ")")
            elif t.get("tenant_type") == "rent":
                label_parts.append("(" + get_text(lang, "tenant_type_rent_short") + ")")
            label = " ".join(p for p in label_parts if p)

            buttons.append(
                [InlineKeyboardButton(label, callback_data=f"regtenant_{t['id']}")]
            )

        await msg.reply_text(
            get_text(lang, "register_choose_tenant").format(apartment=apartment),
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return

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
        # First check duplicate
        dup_info = api_check_duplicate(cat_label)
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

        tenant = api_get_tenant_by_chat(chat_id)
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
    chat_id = update.effective_chat.id
    lang = api_get_user_language(chat_id)

    # Check if already linked
    tenant = api_get_tenant_by_chat(chat_id)
    if tenant:
        txt = get_text(lang, "register_already_linked").format(
            name=tenant.get("name", ""),
            apartment=tenant.get("apartment") or "",
        )
        await update.message.reply_text(txt)
        return

    # Ask for apartment
    context.user_data["awaiting_apartment"] = True
    await update.message.reply_text(get_text(lang, "register_prompt"))

async def photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    chat_id = msg.chat_id
    lang = api_get_user_language(chat_id)

    caption = msg.caption or ""
    if not caption.strip():
        await msg.reply_text(get_text(lang, "photo_need_caption"))
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

    tenant = api_get_tenant_by_chat(chat_id)
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
        "🛠️ *ShahenBot – Available Commands:*\n\n"
        "/start – Main menu\n"
        "/register – Link your apartment\n"
        "/mytickets – Show your tickets\n"
        "/help – Show this help menu\n\n"
        "You can also:\n"
        "• Send a message describing a problem\n"
        "• Send a photo with a caption\n"
        "• Use the buttons to report noise / parking / water / elevator issues\n"
    )

    await update.message.reply_text(text, parse_mode="Markdown")


def main():
    load_messages()
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("register", register))
    app.add_handler(CommandHandler("mytickets", mytickets))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_handler(MessageHandler(filters.PHOTO & ~filters.COMMAND, photo_handler))   
    app.add_handler(CommandHandler("help", help_cmd)) 
    app.add_error_handler(error_handler)

    print(f"ShahenBot is running. API base: {API_BASE_URL}")
    app.run_polling()


if __name__ == "__main__":
    main()
