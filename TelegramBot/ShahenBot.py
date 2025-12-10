import logging
import os
import json
from pathlib import Path

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


def api_create_ticket(chat_id: int, lang: str, category: str, description: str):
    try:
        url = f"{API_BASE_URL}/api/tickets"
        payload = {
            "chat_id": chat_id,
            "category": category,
            "description": description,
            "language": lang,
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

    lang_keyboard = [
        [
            InlineKeyboardButton(get_text(lang, "lang_button_he"), callback_data="lang_he"),
            InlineKeyboardButton(get_text(lang, "lang_button_en"), callback_data="lang_en"),
            InlineKeyboardButton(get_text(lang, "lang_button_fr"), callback_data="lang_fr"),
        ],
    ]
    main_keyboard = [
        [InlineKeyboardButton(get_text(lang, "btn_report"), callback_data="report")]
    ]

    await update.message.reply_text(
        get_text(lang, "start"),
        reply_markup=InlineKeyboardMarkup(lang_keyboard + main_keyboard),
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

        keyboard = [
            [InlineKeyboardButton(get_text(lang, "btn_report"), callback_data="report")]
        ]
        await query.edit_message_text(
            text=f"{text}\n\n{get_text(lang, 'main_menu')}",
            reply_markup=InlineKeyboardMarkup(keyboard),
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

        ticket = api_create_ticket(
            chat_id=chat_id,
            lang=pending_lang,
            category=category,
            description=description,
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
        context.user_data["pending_ticket"] = {
            "category": cat_label,
            "description": text,
            "lang": lang,
        }

        confirm_text = get_text(lang, "auto_detect_proposed").format(
            category=cat_label,
            desc=text,
        )

        keyboard = [
            [
                InlineKeyboardButton(get_text(lang, "btn_yes"), callback_data="confirm_yes"),
                InlineKeyboardButton(get_text(lang, "btn_no"), callback_data="confirm_no"),
            ]
        ]

        await msg.reply_text(
            confirm_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    # In groups, ignore messages without category
    if chat_type in ("group", "supergroup"):
        return

    # In private chat, show main menu
    lang_keyboard = [
        [
            InlineKeyboardButton(get_text(lang, "lang_button_he"), callback_data="lang_he"),
            InlineKeyboardButton(get_text(lang, "lang_button_en"), callback_data="lang_en"),
            InlineKeyboardButton(get_text(lang, "lang_button_fr"), callback_data="lang_fr"),
        ],
    ]
    main_keyboard = [
        [InlineKeyboardButton(get_text(lang, "btn_report"), callback_data="report")]
    ]
    await msg.reply_text(
        get_text(lang, "main_menu"),
        reply_markup=InlineKeyboardMarkup(lang_keyboard + main_keyboard),
    )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Error while handling update:", exc_info=context.error)


def main():
    load_messages()
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_error_handler(error_handler)

    print(f"ShahenBot is running. API base: {API_BASE_URL}")
    app.run_polling()


if __name__ == "__main__":
    main()
