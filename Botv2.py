# -*- coding: utf-8 -*-
"""
PtoShkinDSU_bot — Telegram-бот (python-telegram-bot v20+), готовый к деплою на Koyeb.

Возможности:
- FAQ из Excel (data/faq.xlsx), поддержка «особых» вкладок (пример: СДП, подписание ПЛ).
- Публикации: /post (с подписью и «ожиданием» вложений до 3 минут), /send, /publish (копия собственного сообщения).
- Привязка целевого чата/темы (TARGET_CHAT_ID/TARGET_THREAD_ID) и команды /settarget, /settopic, /bindhere.
- Предложения (кнопка) с логом в CSV и уведомлением админам или в SUGGEST_CHAT_ID.
- Аудит в data/audit.csv (и опционально уведомления в AUDIT_CHAT_ID).
- Чистки: /deleteme, /cleanlast, /cleanhere, /cleanchat, /purgehere.
- Webhook при наличии BASE_URL, иначе polling—для локальной отладки.

Переменные окружения (минимум):
- BOT_TOKEN (обязательно)
- BASE_URL — публичный адрес сервиса (https://<app>.koyeb.app), включает webhook-режим; без него — polling
- PORT — задаёт Koyeb автоматически, код его берёт (не обязательно задавать вручную)
- (опц.) WEBHOOK_SECRET — секрет для валидации входящих POST от Telegram
- (опц.) FAQ_XLSX_PATH — путь до xlsx (по умолчанию data/faq.xlsx)
- (опц.) TARGET_CHAT_ID, SUGGEST_CHAT_ID, POST_ADMINS, SUGGEST_ADMINS, AUDIT_CHAT_ID
"""

import os
import re
import time
import html
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from collections import deque
import datetime
from enum import Enum

import pandas as pd
from dotenv import load_dotenv

from telegram import Update, ReplyKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder, Application, ContextTypes,
    CommandHandler, MessageHandler, filters
)

# ---------- Утилиты времени/константы ----------
def now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)

MAX_DELETE_AGE_SEC = 48 * 3600       # лимит Телеграма на удаление «чужих» сообщений
MAX_POST_WAIT_SEC = 180              # ожидание вложений после /post (сек)
RECENT_MAX = 1000                    # сколько последних сообщений хранить в буфере на чат/тему

# ---------- Конфиг / окружение ----------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Не найден BOT_TOKEN в .env!")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
XLSX_PATH = Path(os.getenv("FAQ_XLSX_PATH") or (DATA_DIR / "faq.xlsx"))

TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID")  # "-100..." или "@channelusername"
TARGET_CHAT_FILE = DATA_DIR / "target_chat.txt"

TARGET_THREAD_ID: Optional[int] = None
TARGET_THREAD_FILE = DATA_DIR / "target_thread.txt"

POST_ADMINS = {int(x) for x in (os.getenv("POST_ADMINS") or "").replace(" ", "").split(",") if x}

SUGGEST_CHAT_ID = os.getenv("SUGGEST_CHAT_ID")
SUGGEST_ADMINS = {int(x) for x in (os.getenv("SUGGEST_ADMINS") or "").replace(" ", "").split(",") if x} or POST_ADMINS

AUDIT_CHAT_ID = os.getenv("AUDIT_CHAT_ID")
AUDIT_CSV = DATA_DIR / "audit.csv"
SUGGESTIONS_CSV = DATA_DIR / "suggestions.csv"

# Опционально: секрет вебхука (рекомендуется для продакшна)
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "") or None

# Пример «особых» вкладок (если твой Excel имеет фиксированную структуру в этих листах)
SPECIAL_BCD_SHEETS = {"Доставка персонала (СДП)", "Подписание путевых листов"}

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger("FAQBot")

# ---------- Кнопочные константы ----------
BTN_HELLO = "🐻 Поздороваться"
BTN_ASK   = "❓ У меня есть вопрос"
BTN_SUGG  = "💡 У меня есть предложение по модернизации данного бота"
BTN_HOWTO = "ℹ️ Как пользоваться ботом"
BTN_BACK  = "⬅️ Назад"

MAIN_KB = ReplyKeyboardMarkup([[BTN_HELLO, BTN_ASK],[BTN_SUGG],[BTN_HOWTO]], resize_keyboard=True)

# ---------- Безопасность / лимиты ----------
STEM_SAFE = re.compile(r"^[\w\-\s\.]+$", re.IGNORECASE)

class Flow(Enum):
    NONE = "none"
    AWAIT_SUGGEST = "await_suggest"

RATE_LIMIT = {"suggest_per_min": 2}
_last_suggest_at: Dict[int, deque] = {}  # uid -> deque[timestamps]

def _ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def _is_safe_stem(s: str) -> bool:
    return bool(STEM_SAFE.match(s or ""))

def _rate_limit_suggest(user_id: int) -> bool:
    dq = _last_suggest_at.setdefault(user_id, deque(maxlen=RATE_LIMIT["suggest_per_min"]))
    now = time.time()
    if dq and len(dq) == dq.maxlen and now - dq[0] < 60:
        return False
    dq.append(now)
    return True

def _sanitize_for_csv(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\r", " ").replace("\n", " ").strip()
    return "'" + s if s[:1] in ("=", "+", "-", "@") else s

def _is_private(update: Update) -> bool:
    return update.effective_chat and update.effective_chat.type == "private"

def _is_post_admin(user_id: int) -> bool:
    return user_id in POST_ADMINS

def _load_target_chat():
    global TARGET_CHAT_ID
    try:
        if TARGET_CHAT_FILE.exists():
            val = TARGET_CHAT_FILE.read_text(encoding="utf-8").strip()
            if val:
                TARGET_CHAT_ID = val
                logger.info("[CONFIG] Загружен TARGET_CHAT_ID из файла: %s", val)
    except Exception:
        logger.exception("Не удалось загрузить target_chat.txt")

def _save_target_chat(chat_id: str):
    try:
        _ensure_data_dir()
        TARGET_CHAT_FILE.write_text(chat_id, encoding="utf-8")
        logger.info("[CONFIG] Сохранён TARGET_CHAT_ID: %s", chat_id)
    except Exception:
        logger.exception("Не удалось сохранить target_chat.txt")

def _load_target_thread():
    global TARGET_THREAD_ID
    try:
        if TARGET_THREAD_FILE.exists():
            val = TARGET_THREAD_FILE.read_text(encoding="utf-8").strip()
            if val:
                TARGET_THREAD_ID = int(val)
                logger.info("[CONFIG] Загружен TARGET_THREAD_ID из файла: %s", val)
    except Exception:
        logger.exception("Не удалось загрузить target_thread.txt")

def _save_target_thread(thread_id: Optional[int]):
    try:
        _ensure_data_dir()
        if thread_id is None:
            TARGET_THREAD_FILE.unlink(missing_ok=True)
            logger.info("[CONFIG] Сброшен TARGET_THREAD_ID")
        else:
            TARGET_THREAD_FILE.write_text(str(thread_id), encoding="utf-8")
            logger.info("[CONFIG] Сохранён TARGET_THREAD_ID: %s", thread_id)
    except Exception:
        logger.exception("Не удалось сохранить target_thread.txt")

def _thread_kwargs():
    return {"message_thread_id": TARGET_THREAD_ID} if TARGET_THREAD_ID else {}

# ---------- Аудит ----------
def _fmt_user(update: Optional[Update]) -> Tuple[str, str]:
    if not update or not update.effective_user:
        return "", ""
    uid = str(update.effective_user.id)
    uname = f"@{update.effective_user.username}" if update.effective_user.username else update.effective_user.full_name
    return uid, uname

def _audit_row(event: str, update: Optional[Update], details: str = "") -> List[str]:
    ts = datetime.datetime.now().isoformat(timespec="seconds")
    uid, uname = _fmt_user(update)
    chat_id = str(update.effective_chat.id) if (update and update.effective_chat) else ""
    chat_type = update.effective_chat.type if (update and update.effective_chat) else ""
    return [ts, uid, uname, chat_id, chat_type, event, details]

def _audit_to_csv(row: List[str]):
    import csv
    _ensure_data_dir()
    new = not AUDIT_CSV.exists()
    with open(AUDIT_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        if new:
            w.writerow(["timestamp", "user_id", "username", "chat_id", "chat_type", "event", "details"])
        row[2] = _sanitize_for_csv(row[2])
        row[6] = _sanitize_for_csv(row[6])
        w.writerow(row)

async def _audit_notify(context: ContextTypes.DEFAULT_TYPE, row: List[str]):
    if not AUDIT_CHAT_ID:
        return
    ts, uid, uname, chat_id, chat_type, event, details = row
    try:
        msg = (
            f"📝 <b>Аудит</b>\n"
            f"Событие: <b>{html.escape(event)}</b>\n"
            f"Пользователь: <code>{uid}</code> {html.escape(uname)}\n"
            f"Чат: <code>{chat_id}</code> ({chat_type})\n"
            f"Детали: {html.escape(details) if details else '—'}\n"
            f"Время: {ts}"
        )
        await context.bot.send_message(chat_id=AUDIT_CHAT_ID, text=msg, parse_mode=ParseMode.HTML)
    except Exception:
        logger.exception("Не удалось отправить аудит-уведомление")

async def _audit(event: str, update: Optional[Update], context: Optional[ContextTypes.DEFAULT_TYPE], details: str = ""):
    row = _audit_row(event, update, details)
    _audit_to_csv(row)
    if context:
        await _audit_notify(context, row)

# ---------- Модель ----------
@dataclass
class FAQItem:
    question: str
    answer: Optional[str] = None
    answer_1: Optional[str] = None
    answer_2: Optional[str] = None
    comment: Optional[str] = None
    files: Optional[List[str]] = None

    def render(self) -> str:
        parts: List[str] = []
        if self.answer_1:
            parts.append(f"<b>Ответ 1:</b> {self.answer_1}")
        if self.answer_2:
            parts.append(f"<b>Ответ 2:</b> {self.answer_2}")
        if self.comment:
            parts.append(f"<i>Комментарий:</i> {self.comment}")
        if self.answer and not parts:
            parts.append(self.answer)
        if not parts:
            parts.append("Ответ не указан.")
        return "\n\n".join(parts)

# ---------- Утилиты Excel/FAQ ----------
def _norm(s: str) -> str:
    return (s or "").strip()

def _pick_one_column(cols: List[str], keywords: List[str]) -> Optional[str]:
    cols_low = [c.lower() for c in cols]
    for kw in keywords:
        kw = kw.lower()
        for i, name in enumerate(cols_low):
            if kw in name:
                return cols[i]
    return None

def _pick_many_columns(cols: List[str], keywords: List[str], exclude: Optional[str] = None) -> List[str]:
    excl = exclude or ""
    out: List[str] = []
    kws = tuple(k.lower() for k in keywords)
    for c in cols:
        if c == excl:
            continue
        name = c.lower()
        if any(k in name for k in kws):
            out.append(c)
    return out

def _split_files_cell(val: str) -> List[str]:
    if not val:
        return []
    raw = re.split(r"[,\n;]+", val)
    return [s.strip() for s in raw if s and s.strip()]

def _split_post_stems(val: str) -> List[str]:
    if not val:
        return []
    raw = re.split(r"[,\n;]+", val)
    return [s.strip() for s in raw if s and s.strip()]

FILE_INDEX: Dict[str, Path] = {}

def _build_file_index():
    FILE_INDEX.clear()
    if DATA_DIR.exists():
        for p in DATA_DIR.iterdir():
            if p.is_file():
                FILE_INDEX[p.stem.lower()] = p
                FILE_INDEX[p.name.lower()] = p

def _find_files_by_stem_fast(stem: str) -> List[Path]:
    s = (stem or "").lower().strip()
    if not s:
        return []
    if s in FILE_INDEX:
        return [FILE_INDEX[s]]
    out = [path for key, path in FILE_INDEX.items() if key.startswith(s)]
    seen, uniq = set(), []
    for p in out:
        if p not in seen:
            uniq.append(p); seen.add(p)
    return uniq

def _append_suggestion(chat_id: int, user_id: int, username: Optional[str], text: str):
    import csv
    _ensure_data_dir()
    is_new = not SUGGESTIONS_CSV.exists()
    with open(SUGGESTIONS_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        if is_new:
            w.writerow(["timestamp", "chat_id", "user_id", "username", "suggestion"])
        safe = _sanitize_for_csv(text)
        w.writerow([datetime.datetime.now().isoformat(timespec="seconds"), chat_id, user_id, username or "", safe])

# ---------- Запоминание последнего сообщения бота ----------
LAST_BOT_MSG: Dict[Tuple[str, int], int] = {}

def _key(chat_id: Optional[int | str], thread_id: Optional[int]) -> Tuple[str, int]:
    return (str(chat_id), int(thread_id) if thread_id else 0)

def _set_last(chat_id: Optional[int | str], thread_id: Optional[int], message_id: int):
    LAST_BOT_MSG[_key(chat_id, thread_id)] = int(message_id)

def _last_target_key() -> Tuple[str, int]:
    return _key(TARGET_CHAT_ID, TARGET_THREAD_ID)

def _find_last_any_thread_for_target() -> Optional[Tuple[Tuple[str, int], int]]:
    items = [(k, v) for k, v in LAST_BOT_MSG.items() if k[0] == str(TARGET_CHAT_ID)]
    if not items:
        return None
    return max(items, key=lambda kv: kv[1])

# --------- Обёртки отправки в TARGET (трекают last) ----------
async def _send_target_message(context: ContextTypes.DEFAULT_TYPE, text: str, **kwargs):
    m = await context.bot.send_message(chat_id=TARGET_CHAT_ID, text=text, **{**_thread_kwargs(), **kwargs})
    _set_last(TARGET_CHAT_ID, TARGET_THREAD_ID, m.message_id)
    return m

async def _send_target_document(context: ContextTypes.DEFAULT_TYPE, document, **kwargs):
    m = await context.bot.send_document(chat_id=TARGET_CHAT_ID, document=document, **{**_thread_kwargs(), **kwargs})
    _set_last(TARGET_CHAT_ID, TARGET_THREAD_ID, m.message_id)
    return m

async def _send_target_photo(context: ContextTypes.DEFAULT_TYPE, photo, **kwargs):
    m = await context.bot.send_photo(chat_id=TARGET_CHAT_ID, photo=photo, **{**_thread_kwargs(), **kwargs})
    _set_last(TARGET_CHAT_ID, TARGET_THREAD_ID, m.message_id)
    return m

# ---------- Буфер недавних сообщений (для /cleanchat и /purgehere) ----------
RECENT_MSGS: Dict[Tuple[str, int], deque] = {}  # ключ=(chat_id, thread_id_or_0) -> deque(dict)

def _recent_key(chat_id: int | str, thread_id: Optional[int]) -> Tuple[str, int]:
    return (str(chat_id), int(thread_id) if thread_id else 0)

def _recent_deque(chat_id: int | str, thread_id: Optional[int]) -> deque:
    key = _recent_key(chat_id, thread_id)
    dq = RECENT_MSGS.get(key)
    if dq is None:
        dq = deque(maxlen=RECENT_MAX)
        RECENT_MSGS[key] = dq
    return dq

async def track_recent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ловим ВСЕ сообщения в группах/темах, чтобы чистки могли работать по буферу."""
    chat = update.effective_chat
    msg = update.message
    if not msg or chat.type not in ("group", "supergroup"):
        return
    dq = _recent_deque(chat.id, msg.message_thread_id)
    mdate = msg.date
    if mdate and mdate.tzinfo is None:
        mdate = mdate.replace(tzinfo=datetime.timezone.utc)
    dq.append({
        "message_id": msg.message_id,
        "from_user_id": msg.from_user.id if msg.from_user else None,
        "date": mdate,
    })

# ---------- Глобальные данные/кэш бота ----------
BOT_INFO = {"id": None, "username": None}

# ---------- Команды ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        await update.message.reply_text("Напиши мне в личку: открой профиль бота и нажми «Message».")
        return
    await update.message.reply_text(
        "Привет! Я <b>PtoShkinDSU_bot</b> 🤖\nВыбирай кнопку ниже 👇",
        reply_markup=MAIN_KB,
        parse_mode=ParseMode.HTML
    )
    await _audit("start", update, context, "Пользователь открыл бота")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        await update.message.reply_text("Эта команда доступна только в личке с ботом.")
        return
    guide = (
        "<b>Как пользоваться ботом</b>\n\n"
        "🧩 <b>FAQ</b>\n"
        "• Нажми «❓ У меня есть вопрос» → выбери категорию → вопрос.\n\n"
        "📌 <b>Публикации</b>\n"
        "• По умолчанию — в целевой чат/канал из <code>TARGET_CHAT_ID</code>.\n\n"
        "📝 <b>Публикация объявления (2 шага)</b>\n"
        "1) В ЛС: <code>/post | Текст объявления</code>\n"
        "2) В течение 3 минут пришли Excel/PDF/картинку — бот прикрепит их.\n"
        "• Можно вместо вложений указать стемы файлов из <code>data/</code>:\n"
        "  <code>/post отчет_октябрь | Сводка</code> — текст попадёт в подпись первого файла.\n\n"
        "⚡ <b>Быстрая отправка</b>\n"
        "• <code>/send Текст</code> — мгновенное сообщение в целевую тему/чат.\n"
        "• <code>/publish</code> — ответь этой командой в ЛС на <i>своё</i> сообщение с медиа — бот скопирует в тему.\n\n"
        "🧹 <b>Удаление</b>\n"
        "• <code>/deleteme</code> (в группе): как ответ — удалит цель и команду; без ответа — только команду.\n"
        "• <code>/cleanlast</code> (в ЛС, для админов): удалит <i>последнее сообщение бота</i> в целевом чате/теме.\n"
        "• <code>/cleanhere</code> (в группе, для админов): удалит <i>последнее сообщение бота</i> в текущем чате/теме.\n"
        "• <code>/cleanchat [N]</code> (в группе, для админов): очистит до N последних сообщений (по буферу), оставив только админов.\n"
        "• <code>/purgehere</code> (в группе, для админов): ответь этой командой на сообщение — удалю всё новее него (в пределах 48 часов, не трогаю админов).\n\n"
        "💡 <b>Предложения</b>\n"
        "• Нажми «💡 У меня есть предложение по модернизации данного бота» и напиши текст — бот уведомит админов.\n\n"
        "🆔 <b>Служебные</b>\n"
        "• <code>/myid</code>, <code>/getchat</code>, <code>/listfiles</code>, <code>/reindex</code>, <code>/settarget</code>, <code>/settopic</code>.\n\n"
        "🔐 <i>Приватность</i>: команды/меню работают в личке; публикации идут в привязанную тему."
    )
    await update.message.reply_text(guide, parse_mode=ParseMode.HTML)
    await _audit("howto", update, context, "guide shown")

async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid, uname = _fmt_user(update)
    await update.message.reply_text(f"👤 Твой Telegram ID: {uid}\nИмя: {uname}")
    await _audit("myid", update, context, "Показ своего ID")

async def getchat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    try:
        await context.bot.send_message(chat_id=update.effective_user.id, text=f"chat_id = {chat.id}")
    except Exception:
        await update.message.reply_text(f"chat_id = {chat.id}")
    if chat.type in ("group", "supergroup"):
        try:
            await context.bot.delete_message(chat_id=chat.id, message_id=update.message.message_id)
        except Exception:
            pass
    await _audit("getchat", update, context, f"chat_id={chat.id}")

# /deleteme — удалить цель/команду
async def deleteme(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from telegram.error import TelegramError

    chat = update.effective_chat
    msg = update.message

    if chat.type not in ("group", "supergroup"):
        if _is_private(update):
            await msg.reply_text("Эта команда работает только в группе.")
        return

    target = msg.reply_to_message or msg
    target_id = target.message_id

    try:
        me = await context.bot.get_me()
        me_id = me.id
        my_member = await context.bot.get_chat_member(chat.id, me_id)
        status = getattr(my_member, "status", "")
        can_delete = (status == "creator") or (status == "administrator" and bool(getattr(my_member, "can_delete_messages", False)))
        if not can_delete and (not msg.reply_to_message or (msg.reply_to_message and msg.reply_to_message.from_user and msg.reply_to_message.from_user.id != me_id)):
            try:
                await msg.reply_text("Мне нужны права администратора с галочкой «Удалять сообщения». "
                                     "Свои сообщения я могу удалить и без этого.")
            except Exception:
                pass
            await _audit("deleteme_no_rights", update, context, f"status={status}")
            return
    except TelegramError as e:
        try:
            await msg.reply_text(f"Не смог проверить права: {e}")
        except Exception:
            pass
        await _audit("deleteme_rights_error", update, context, str(e))
        return

    try:
        me = await context.bot.get_me()
        is_own = (target.from_user and target.from_user.id == me.id)
        if not is_own:
            tdate = target.date
            if tdate and tdate.tzinfo is None:
                tdate = tdate.replace(tzinfo=datetime.timezone.utc)
            if tdate and (now_utc() - tdate).total_seconds() > MAX_DELETE_AGE_SEC:
                try:
                    await msg.reply_text("Нельзя удалить: сообщению больше 48 часов.")
                except Exception:
                    pass
                await _audit("deleteme_too_old", update, context, "")
                return
    except Exception as e:
        await _audit("deleteme_age_check_error", update, context, str(e))

    try:
        await context.bot.delete_message(chat_id=chat.id, message_id=target_id)
        if msg.reply_to_message:
            try:
                await context.bot.delete_message(chat_id=chat.id, message_id=msg.message_id)
            except Exception:
                pass
        await _audit("deleteme_ok", update, context, f"deleted_msg_id={target_id}")
    except Exception as e:
        try:
            await msg.reply_text(f"❌ Не смог удалить: {e}")
        except Exception:
            pass
        await _audit("deleteme_error", update, context, str(e))

# cleanlast / cleanhere
async def cleanlast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        await update.message.reply_text("Эта команда доступна только в личке с ботом.")
        return
    uid = update.effective_user.id if update.effective_user else 0
    if uid not in POST_ADMINS:
        await update.message.reply_text("⛔ Нет прав.")
        return

    key = _last_target_key()
    msg_id = LAST_BOT_MSG.get(key)
    use_chat = TARGET_CHAT_ID
    use_thread = TARGET_THREAD_ID

    if not msg_id:
        found = _find_last_any_thread_for_target()
        if found:
            key, msg_id = found
            use_chat = key[0]
            use_thread = key[1] if key[1] != 0 else None

    if not (use_chat and msg_id):
        await update.message.reply_text("Не найдено последнее сообщение бота для целевого чата/темы.")
        await _audit("cleanlast_no_msg", update, context, f"key={_last_target_key()}")
        return

    try:
        await context.bot.delete_message(chat_id=use_chat, message_id=msg_id)
        await update.message.reply_text("🧹 Удалил последнее сообщение бота.")
        await _audit("cleanlast_ok", update, context, f"deleted_msg_id={msg_id}")
        LAST_BOT_MSG.pop(key, None)
    except Exception as e:
        await update.message.reply_text(f"❌ Не смог удалить: {e}")
        await _audit("cleanlast_error", update, context, str(e))

async def cleanhere(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет последнее СВОЁ сообщение бота в текущем чате/теме (то, что помнит трекер отправок)."""
    chat = update.effective_chat
    uid = update.effective_user.id if update.effective_user else 0
    if uid not in POST_ADMINS:
        return
    thread_id = update.message.message_thread_id
    key = _key(chat.id, thread_id)
    msg_id = LAST_BOT_MSG.get(key)
    if not msg_id:
        try:
            await update.message.reply_text("Здесь ещё нет моих сообщений, которые я помню.")
        except Exception:
            pass
        await _audit("cleanhere_no_msg", update, context, f"key={key}")
        return
    try:
        await context.bot.delete_message(chat_id=chat.id, message_id=msg_id)
        try:
            await context.bot.delete_message(chat_id=chat.id, message_id=update.message.message_id)
        except Exception:
            pass
        await _audit("cleanhere_ok", update, context, f"deleted_msg_id={msg_id}")
        LAST_BOT_MSG.pop(key, None)
    except Exception as e:
        try:
            await update.message.reply_text(f"❌ Не смог удалить: {e}")
        except Exception:
            pass
        await _audit("cleanhere_error", update, context, str(e))

# ---------- /cleanchat — разовая очистка истории (по буферу RECENT) ----------
async def cleanchat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удалить последние N сообщений в текущей теме/чате, оставив только админов."""
    from telegram.error import TelegramError

    chat = update.effective_chat
    msg = update.message
    uid = update.effective_user.id if update.effective_user else 0

    if chat.type not in ("group", "supergroup"):
        await msg.reply_text("Команду /cleanchat нужно вызывать в группе/теме.")
        return

    if uid not in POST_ADMINS:
        await msg.reply_text("⛔ Нет прав.")
        return

    try:
        limit = int(context.args[0]) if context.args else 200
    except ValueError:
        limit = 200
    limit = max(1, min(limit, RECENT_MAX))

    try:
        admins = await context.bot.get_chat_administrators(chat.id)
        admin_ids = {a.user.id for a in admins}

        thread_id = msg.message_thread_id
        dq = _recent_deque(chat.id, thread_id)

        if not dq:
            await msg.reply_text("Буфер пуст — нечего чистить (бот не видел сообщений).")
            return

        now = now_utc()
        deleted = 0
        checked = 0
        for item in reversed(list(dq)[-limit:]):
            checked += 1
            uid_from = item.get("from_user_id")
            mid = item.get("message_id")
            mdate = item.get("date")
            if uid_from in admin_ids:
                continue
            if isinstance(mdate, datetime.datetime):
                if mdate.tzinfo is None:
                    mdate = mdate.replace(tzinfo=datetime.timezone.utc)
                if (now - mdate).total_seconds() > MAX_DELETE_AGE_SEC:
                    continue
            try:
                await context.bot.delete_message(chat.id, mid)
                deleted += 1
            except TelegramError:
                pass

        await msg.reply_text(f"✅ Проверено {checked} последних сообщений из буфера, удалено {deleted}.")
        await _audit("cleanchat_ok", update, context, f"checked={checked}; deleted={deleted}")
    except Exception as e:
        await msg.reply_text(f"❌ Ошибка очистки: {e}")
        await _audit("cleanchat_error", update, context, str(e))

# ---------- /purgehere — удалить всё новее сообщения-«якоря» ----------
async def purgehere(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Ответь этой командой на СООБЩЕНИЕ — бот удалит все (виденные им) сообщения НОВЕЕ якоря,
    кроме сообщений админов и старше 48 часов.
    """
    from telegram.error import TelegramError

    chat = update.effective_chat
    msg = update.message
    uid = update.effective_user.id if update.effective_user else 0

    if chat.type not in ("group", "supergroup"):
        await msg.reply_text("Команду нужно вызывать в группе/теме.")
        return
    if uid not in POST_ADMINS:
        await msg.reply_text("⛔ Нет прав.")
        return
    if not msg.reply_to_message:
        await msg.reply_text("Ответь этой командой на СООБЩЕНИЕ, до которого нужно очистить (я удалю всё НОВЕЕ него).")
        return

    admins = await context.bot.get_chat_administrators(chat.id)
    admin_ids = {a.user.id for a in admins}

    thread_id = msg.message_thread_id
    dq = _recent_deque(chat.id, thread_id)
    if not dq:
        await msg.reply_text("Буфер пуст — нечего чистить (бот не видел сообщений).")
        return

    anchor_id = msg.reply_to_message.message_id
    now = now_utc()

    to_delete: List[int] = []
    for item in reversed(list(dq)):
        mid = item["message_id"]
        if mid <= anchor_id:
            break
        if item.get("from_user_id") in admin_ids:
            continue
        mdate = item.get("date")
        if isinstance(mdate, datetime.datetime):
            if mdate.tzinfo is None:
                mdate = mdate.replace(tzinfo=datetime.timezone.utc)
            if (now - mdate).total_seconds() > MAX_DELETE_AGE_SEC:
                continue
        to_delete.append(mid)

    deleted = 0
    for mid in to_delete:
        try:
            await context.bot.delete_message(chat.id, mid)
            deleted += 1
        except TelegramError:
            pass

    # стараемся убрать саму команду
    try:
        await context.bot.delete_message(chat.id, msg.message_id)
    except Exception:
        pass

    # Сообщение-итог (учтём как «последнее» бота в этой теме)
    try:
        m = await context.bot.send_message(chat.id, f"🧹 Готово. Удалено: {deleted}.", message_thread_id=thread_id)
        _set_last(chat.id, thread_id, m.message_id)
    except Exception:
        pass

    await _audit("purgehere", update, context, f"deleted={deleted}; anchor={anchor_id}")

async def listfiles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        await update.message.reply_text("Эта команда доступна только в личке с ботом.")
        return
    if not DATA_DIR.exists():
        await update.message.reply_text("Папка data/ не найдена.")
        await _audit("listfiles", update, context, "data/ not found")
        return
    files = [p.name for p in DATA_DIR.iterdir() if p.is_file()]
    if not files:
        await update.message.reply_text("В папке data/ файлов нет.")
    else:
        msg = "📂 Файлы в data/:\n" + "\n".join(f"• {f}" for f in files)
        await update.message.reply_text(msg)
    await _audit("listfiles", update, context, f"count={len(files)}")

async def settarget(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        await update.message.reply_text("Эта команда доступна только в личке с ботом.")
        return
    uid = update.effective_user.id if update.effective_user else 0
    if uid not in POST_ADMINS:
        await update.message.reply_text("⛔ У тебя нет прав менять целевую группу.")
        return
    if not context.args:
        await update.message.reply_text("Использование: /settarget <chat_id>\nНапример: /settarget -1002454786265")
        return
    new_id = context.args[0].strip()
    global TARGET_CHAT_ID
    TARGET_CHAT_ID = new_id
    _save_target_chat(new_id)
    await update.message.reply_text(f"✅ TARGET_CHAT_ID обновлён: {new_id}")
    await _audit("settarget", update, context, f"TARGET_CHAT_ID={new_id}")

async def settopic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        await update.message.reply_text("Команда доступна только в личке.")
        return
    uid = update.effective_user.id if update.effective_user else 0
    if uid not in POST_ADMINS:
        await update.message.reply_text("⛔ Нет прав.")
        return
    global TARGET_THREAD_ID
    if not context.args:
        await update.message.reply_text("Использование: /settopic <thread_id> | /settopic 0 (сброс)")
        return
    try:
        val = int(context.args[0])
    except ValueError:
        await update.message.reply_text("thread_id должен быть числом.")
        return
    TARGET_THREAD_ID = None if val == 0 else val
    _save_target_thread(TARGET_THREAD_ID)
    await update.message.reply_text(f"✅ TARGET_THREAD_ID = {TARGET_THREAD_ID}")
    await _audit("settopic", update, context, f"TARGET_THREAD_ID={TARGET_THREAD_ID}")

async def bindhere(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Команда оставлена, но из гайда убрана.
    chat = update.effective_chat
    uid = update.effective_user.id if update.effective_user else 0
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("Вызови эту команду в теме группы, куда надо публиковать.")
        return
    if uid not in POST_ADMINS:
        await update.message.reply_text("⛔ Нет прав.")
        return
    thread_id = update.message.message_thread_id
    if not thread_id:
        await update.message.reply_text("Команду нужно вызвать ВНУТРИ темы (не в списке тем).")
        return
    global TARGET_CHAT_ID, TARGET_THREAD_ID
    TARGET_CHAT_ID = str(chat.id)
    _save_target_chat(TARGET_CHAT_ID)
    TARGET_THREAD_ID = int(thread_id)
    _save_target_thread(TARGET_THREAD_ID)
    await update.message.reply_text(
        f"✅ Привязано сюда.\nchat_id={TARGET_CHAT_ID}\nthread_id={TARGET_THREAD_ID}\nТеперь все публикации пойдут в эту тему."
    )
    await _audit("bindhere", update, context, f"chat={TARGET_CHAT_ID}, thread={TARGET_THREAD_ID}")

# ---------- Сбор вложений из сообщения (для /post) ----------
def _collect_attachments_from_message(update: Update) -> List[Dict[str, str]]:
    """Возвращает список вложений текущего сообщения:
       [{"type": "document"|"photo", "file_id": "<id>"}]"""
    atts: List[Dict[str, str]] = []
    msg = update.message
    if not msg:
        return atts
    if msg.document:
        atts.append({"type": "document", "file_id": msg.document.file_id})
    if msg.photo:
        best = max(msg.photo, key=lambda p: (p.file_size or 0))
        atts.append({"type": "photo", "file_id": best.file_id})
    return atts

# ---------- Публикация /post ----------
POST_PENDING: Dict[int, Dict[str, object]] = {}

async def cmd_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        await update.message.reply_text("Эта команда доступна только в личке с ботом.")
        return
    uid = update.effective_user.id if update.effective_user else 0
    if not _is_post_admin(uid):
        await update.message.reply_text("⛔ У тебя нет прав на публикацию.")
        return

    raw_all = (update.message.text or update.message.caption or "").strip()
    after = raw_all.split(" ", 1)[1] if raw_all.lower().startswith("/post") and " " in raw_all else raw_all

    if "|" in after:
        stem_part, desc_part = after.split("|", 1)
        stems = _split_post_stems(stem_part.strip())
        desc = desc_part.strip()
    else:
        stems, desc = [], after.strip()

    attachments = _collect_attachments_from_message(update)

    if not attachments:
        POST_PENDING[update.effective_chat.id] = {"desc": desc, "stems": stems, "ts": time.time()}
        await update.message.reply_text(
            "Принято. Жду файл(ы) Excel/PDF/картинку следующими сообщениями (до 3 минут). "
            "Также можно указать стемы файлов из data/. Как пришлёшь — опубликую."
        )

    await _audit("post_command", update, context, f"desc_len={len(desc)}; stems={','.join(stems) if stems else '-'}")
    await _do_publish(update, context, desc, stems, attachments)

async def _do_publish(update: Update, context: ContextTypes.DEFAULT_TYPE, desc: str, stems: List[str], attachments: List[Dict[str, str]]):
    files_from_data: List[Path] = []
    missing: List[str] = []
    for stem in stems:
        if not _is_safe_stem(stem):
            await update.message.reply_text(f"Недопустимое имя файла: {stem}")
            await _audit("post_error", update, context, f"unsafe_stem={stem}")
            return
        matched = _find_files_by_stem_fast(stem)
        (files_from_data.append(matched[0]) if matched else missing.append(stem))
    if missing:
        await update.message.reply_text("⚠️ Не найдены файлы: " + ", ".join(missing))

    try:
        sent_any = False

        if attachments:
            for i, att in enumerate(attachments):
                cap = desc if (i == 0 and desc) else None
                if att["type"] == "document":
                    await _send_target_document(context, att["file_id"], caption=cap, parse_mode=ParseMode.HTML if cap else None)
                elif att["type"] == "photo":
                    await _send_target_photo(context, att["file_id"], caption=cap, parse_mode=ParseMode.HTML if cap else None)
            sent_any = True

        if files_from_data:
            if not sent_any and desc:
                first, *rest = files_from_data
                with open(first, "rb") as f:
                    await _send_target_document(context, f, filename=first.name, caption=desc, parse_mode=ParseMode.HTML)
                for p in rest:
                    with open(p, "rb") as f:
                        await _send_target_document(context, f, filename=p.name)
            else:
                for p in files_from_data:
                    with open(p, "rb") as f:
                        await _send_target_document(context, f, filename=p.name)
            sent_any = True

        if not sent_any:
            if desc:
                await _send_target_message(context, text=desc, parse_mode=ParseMode.HTML)
            else:
                await update.message.reply_text(
                    "Нечего публиковать: ни вложений, ни стемов, ни описания.\n"
                    "Использование: /post <стем[,стем2]> | <описание> — или прикрепи файл(ы)."
                )
                await _audit("post_error", update, context, "nothing to publish")
                return

        await update.message.reply_text("✅ Опубликовано.")
        await _audit("post_published", update, context, f"desc_len={len(desc)}; att={len(attachments)}; files={len(files_from_data)}")
    except Exception as e:
        logger.exception("Ошибка публикации: %s", e)
        await update.message.reply_text(f"❌ Ошибка публикации: {e}")
        await _audit("post_error", update, context, str(e))

async def capture_post_attachments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        return
    chat_id = update.effective_chat.id
    pending = POST_PENDING.get(chat_id)
    if not pending:
        return
    if time.time() - float(pending.get("ts", 0)) > MAX_POST_WAIT_SEC:
        POST_PENDING.pop(chat_id, None)
        return

    atts = _collect_attachments_from_message(update)
    if not atts:
        return

    desc = str(pending.get("desc") or "")
    stems = list(pending.get("stems") or [])
    POST_PENDING.pop(chat_id, None)

    await _audit("post_attachments", update, context, f"count={len(atts)}; desc_len={len(desc)}")
    await _do_publish(update, context, desc, stems, atts)

# ---------- FAQ / кнопки / предложения ----------
async def howto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # уже показано выше как guide; эта функция оставлена для кнопки "ℹ️"
    return await help_cmd(update, context)

async def crab(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        return
    await update.message.reply_text("Привет, лови краба от моей медвежьей лапы! 🦀🐻")
    await _audit("button_hello", update, context, "crab")

# ---------- FAQ репозиторий ----------
@dataclass
class _FAQRepo:
    xlsx_path: Path
    data: Dict[str, List[FAQItem]]

class FAQRepository:
    def __init__(self, xlsx_path: Path):
        self.xlsx_path = xlsx_path
        self.data: Dict[str, List[FAQItem]] = {}

    def load(self) -> None:
        if not self.xlsx_path.exists():
            raise FileNotFoundError(f"Не найден файл: {self.xlsx_path}")
        book: Dict[str, pd.DataFrame] = pd.read_excel(
            self.xlsx_path, sheet_name=None, dtype=str, engine="openpyxl"
        )
        normalized: Dict[str, List[FAQItem]] = {}
        for sheet, df in book.items():
            if df is None or df.empty:
                continue
            df = df.replace(r"^\s*$", pd.NA, regex=True).dropna(how="all").fillna("")
            if df.empty:
                continue
            if sheet.strip() in SPECIAL_BCD_SHEETS:
                items = self._parse_special_bcd(df)
            else:
                items = self._parse_generic(df)
            items = [it for it in items if it.question and it.question.strip()]
            if items:
                normalized[sheet] = items
        if not normalized:
            raise RuntimeError("Не удалось извлечь FAQ ни с одной вкладки Excel.")
        self.data = normalized

    def _extract_files_from_row(self, df: pd.DataFrame, row: pd.Series) -> List[str]:
        file_cols = [c for c in df.columns if ("файл" in c.lower()) or ("file" in c.lower())]
        if not file_cols:
            return []
        cell = _norm(str(row[file_cols[0]]))
        if not cell or cell.lower() == "nan":
            return []
        return _split_files_cell(cell)

    def _parse_special_bcd(self, df: pd.DataFrame) -> List[FAQItem]:
        if df.shape[1] < 4:
            return []
        items: List[FAQItem] = []
        for _, row in df.iterrows():
            q  = _norm(str(row.iloc[0]))
            a1 = _norm(str(row.iloc[1])) or None
            a2 = _norm(str(row.iloc[2])) or None
            cm = _norm(str(row.iloc[3])) or None
            if not q:
                continue
            files = self._extract_files_from_row(df, row)
            items.append(FAQItem(question=q, answer_1=a1, answer_2=a2, comment=cm, files=files or None))
        return items

    def _parse_generic(self, df: pd.DataFrame) -> List[FAQItem]:
        cols = list(df.columns)
        q_col = _pick_one_column(cols, ["вопрос", "тема", "наименование", "заголовок"]) or cols[0]
        answer_cols = _pick_many_columns(
            cols,
            ["ответ", "описание", "информация", "что делать", "как", "где",
             "контакт", "телефон", "email", "почта", "ссылка", "адрес", "комментар"],
            exclude=q_col
        ) or (cols[1:2] if len(cols) > 1 else [])
        items: List[FAQItem] = []

        for _, row in df.iterrows():
            q = _norm(str(row[q_col]))
            if not q:
                continue
            parts: List[str] = []
            multi = len(answer_cols) > 1
            for c in answer_cols:
                val = _norm(str(row[c]))
                if val and val.lower() != "nan":
                    parts.append(f"<b>{c}:</b> {val}" if multi else val)
            answer_text = "\n\n".join(parts) if parts else None
            files = self._extract_files_from_row(df, row)
            if answer_text or files:
                items.append(FAQItem(question=q, answer=answer_text, files=files or None))
        return items

# ---------- Глобальные данные ----------
repo = FAQRepository(XLSX_PATH)
repo.load()
CATEGORIES: List[str] = list(repo.data.keys())
ALL_QUESTIONS: List[Tuple[str, str]] = [(cat, it.question) for cat, items in repo.data.items() for it in items]
USER_CATEGORY: Dict[int, Optional[str]] = {}
USER_FLOW: Dict[int, Flow] = {}

# ---------- Предложения ----------
async def suggest_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        await update.message.reply_text("Эта функция доступна только в личке с ботом.")
        return
    uid = update.effective_user.id
    USER_FLOW[uid] = Flow.AWAIT_SUGGEST
    await update.message.reply_text(
        "Напиши, пожалуйста, своё предложение одним сообщением.\n"
        "Можно приложить ссылки/описания. После отправки я всё перекину админу. ✍️"
    )
    await _audit("suggest_start", update, context, "")

async def suggest_capture(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        return
    uid = update.effective_user.id
    if USER_FLOW.get(uid) != Flow.AWAIT_SUGGEST:
        return

    txt = (update.message.text or "").strip()
    if not txt:
        return

    if not _rate_limit_suggest(uid):
        await update.message.reply_text("Слишком часто. Подожди чуть-чуть и отправь снова 🙏")
        await _audit("suggest_ratelimited", update, context, "")
        return

    _append_suggestion(
        chat_id=update.effective_chat.id,
        user_id=uid,
        username=f"@{update.effective_user.username}" if update.effective_user.username else update.effective_user.full_name,
        text=txt
    )

    notified = 0
    try:
        msg = (
            "💡 <b>Новое предложение</b>\n\n"
            f"От: <code>{uid}</code> "
            f"{html.escape('@'+update.effective_user.username) if update.effective_user.username else html.escape(update.effective_user.full_name)}\n\n"
            f"{html.escape(txt)}"
        )
        if SUGGEST_CHAT_ID:
            try:
                await context.bot.send_message(chat_id=SUGGEST_CHAT_ID, text=msg, parse_mode=ParseMode.HTML)
                notified += 1
            except Exception:
                logger.exception("Не удалось отправить в SUGGEST_CHAT_ID")
        else:
            for admin_id in SUGGEST_ADMINS:
                try:
                    await context.bot.send_message(chat_id=admin_id, text=msg, parse_mode=ParseMode.HTML)
                    notified += 1
                except Exception:
                    pass
    except Exception:
        logger.exception("Ошибка отправки уведомления админам")

    USER_FLOW[uid] = Flow.NONE
    await update.message.reply_text("✅ Спасибо! Твоё предложение передано.")
    await _audit("suggest_captured", update, context, f"notified={notified}")

# --- Хук старта ---
async def _on_start(app: Application):
    _build_file_index()
    _load_target_chat()
    _load_target_thread()
    # Сбрасываем старый вебхук (важно при миграциях/перезапусках)
    await app.bot.delete_webhook(drop_pending_updates=True)

    me = await app.bot.get_me()
    BOT_INFO["id"] = me.id
    BOT_INFO["username"] = me.username
    logger.info("Bot started as @%s (id=%s), target=%s, thread=%s", me.username, me.id, TARGET_CHAT_ID, TARGET_THREAD_ID)

# ---------- Роутинг ----------
def build_app() -> Application:
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(_on_start).build()

    # Команды
    app.add_handler(CommandHandler("getchat", getchat), group=0)
    app.add_handler(CommandHandler("start", start, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("help", help_cmd, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("listfiles", listfiles, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("myid", myid), group=0)
    app.add_handler(CommandHandler("post", cmd_post, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("send", send_text, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("publish", publish_reply, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("reindex", cmd_reindex, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("settarget", settarget, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("settopic", settopic, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("bindhere", bindhere), group=0)
    app.add_handler(CommandHandler("deleteme", deleteme), group=0)
    app.add_handler(CommandHandler("cleanlast", cleanlast, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("cleanhere", cleanhere), group=0)
    app.add_handler(CommandHandler("cleanchat", cleanchat), group=0)
    app.add_handler(CommandHandler("purgehere", purgehere), group=0)

    # Кнопки — только в ЛС
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & filters.TEXT & filters.Regex(r"^🐻 (Поздороваться|Лови краба)$"),
        crab
    ), group=1)
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & filters.TEXT & filters.Regex(rf"^{re.escape(BTN_ASK)}$"),
        lambda u, c: u.message.reply_text("Выбери категорию 👇", reply_markup=ReplyKeyboardMarkup([[x] for x in CATEGORIES] + [[BTN_BACK]], resize_keyboard=True))
    ), group=1)
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & filters.TEXT & filters.Regex(rf"^{re.escape(BTN_SUGG)}$"),
        lambda u, c: suggest_start(u, c)
    ), group=1)
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & filters.TEXT & filters.Regex(rf"^{re.escape(BTN_HOWTO)}$"),
        help_cmd
    ), group=1)
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & filters.TEXT & filters.Regex(rf"^{re.escape(BTN_BACK)}$"),
        lambda u, c: u.message.reply_text("Главное меню 👇", reply_markup=MAIN_KB)
    ), group=1)

    # Вложения после /post — только в ЛС
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & (filters.Document.ALL | filters.PHOTO) & ~filters.COMMAND,
        capture_post_attachments
    ), group=1)

    # Предложения — только в ЛС
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND,
        suggest_capture
    ), group=2)

    # Трекер сообщений для чисток — во всех группах/темах
    app.add_handler(MessageHandler(
        filters.ChatType.GROUPS & ~filters.COMMAND,
        track_recent
    ), group=9)

    return app

# ---------- Служебные (оставлены без изменений) ----------
async def send_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        await update.message.reply_text("Эта команда доступна только в личке с ботом.")
        return
    user_id = update.effective_user.id if update.effective_user else 0
    if not _is_post_admin(user_id):
        await update.message.reply_text("⛔ У тебя нет прав на публикацию.")
        return
    raw = (update.message.text or "").strip()
    payload = raw.split(" ", 1)[1].strip() if " " in raw else ""
    if not payload:
        await update.message.reply_text("Использование: /send <текст сообщения>")
        return
    try:
        await _send_target_message(context, text=payload, parse_mode=ParseMode.HTML)
        await update.message.reply_text("✅ Отправлено в группу.")
        await _audit("send", update, context, f"text_len={len(payload)}")
    except Exception as e:
        logger.exception("Ошибка отправки текста: %s", e)
        await update.message.reply_text(f"❌ Ошибка отправки: {e}")
        await _audit("send_error", update, context, str(e))

async def publish_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        await update.message.reply_text("Эта команда доступна только в личке с ботом.")
        return
    user_id = update.effective_user.id if update.effective_user else 0
    if not _is_post_admin(user_id):
        await update.message.reply_text("⛔ У тебя нет прав на публикацию.")
        return
    msg = update.message
    if not msg.reply_to_message:
        await msg.reply_text("Ответь этой командой на сообщение (с текстом/медиа), которое надо опубликовать.")
        return
    if not msg.reply_to_message.from_user or msg.reply_to_message.from_user.id != user_id:
        await msg.reply_text("Можно публиковать только собственные сообщения.")
        return
    try:
        mid = await context.bot.copy_message(
            chat_id=TARGET_CHAT_ID,
            from_chat_id=msg.chat.id,
            message_id=msg.reply_to_message.message_id,
            **_thread_kwargs()
        )
        _set_last(TARGET_CHAT_ID, TARGET_THREAD_ID, mid.message_id)
        await msg.reply_text("✅ Опубликовано в группу.")
        await _audit("publish", update, context, "copy_message")
    except Exception as e:
        logger.exception("Ошибка публикации копией: %s", e)
        await msg.reply_text(f"❌ Ошибка публикации: {e}")
        await _audit("publish_error", update, context, str(e))

async def cmd_reindex(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        await update.message.reply_text("Эта команда доступна только в личке с ботом.")
        return
    u = update.effective_user
    if not u or u.id not in POST_ADMINS:
        await update.message.reply_text("⛔ Нет прав.")
        return
    _build_file_index()
    await update.message.reply_text("🔄 Индекс файлов пересобран.")
    await _audit("reindex", update, context, "rebuild file index")

# ---------- Точка входа ----------
if __name__ == "__main__":
    print(f"[DEBUG] BASE_DIR: {BASE_DIR}")
    print(f"[DEBUG] XLSX_PATH: {XLSX_PATH} (exists={XLSX_PATH.exists()})")
    app = build_app()
    print("Bot is starting…")

    BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
    port = int(os.getenv("PORT", "8000"))

    if BASE_URL:
        webhook_path = f"/{BOT_TOKEN}"
        full_url = f"{BASE_URL}{webhook_path}"
        print(f"[DEBUG] Using WEBHOOK at {full_url} (port={port})")
        # Вебхук-режим для Koyeb
        app.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=BOT_TOKEN,
            webhook_url=full_url,
            secret_token=WEBHOOK_SECRET,
            drop_pending_updates=True,
            allowed_updates=["message"],
            stop_signals=None,
        )
    else:
        print("[DEBUG] Using POLLING mode")
        app.run_polling(
            close_loop=False,
            drop_pending_updates=True,
            allowed_updates=["message"],
            stop_signals=None,
        )