#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PtoShkinDSU_bot — Telegram-бот (python-telegram-bot v20+)

Возможности:
- FAQ из Excel (data/faq.xlsx), поддержка «особых» вкладок A/B/C/D.
- Публикации: /post (с caption и «ожиданием» вложений до 3 минут), /send, /publish (только «своё»), /settarget.
- Темы (форумы): /bindhere (привязка к текущей теме), /settopic <thread_id|0> (ручная настройка/сброс).
- Предложения: текст → уведомление админам, лог в CSV (с безопасностью).
- Аудит: лог в data/audit.csv и (опц.) уведомления в AUDIT_CHAT_ID — кто заходил, что смотрел, что публиковал.
- /deleteme: удалить сообщение в группе (как ответ — удалит цель и команду; иначе — только команду). Даёт понятные причины, если не получилось.
- Privacy Mode OFF: команды/диалоги — только в ЛС; публикации идут в TARGET_CHAT_ID(+опц. thread).
- /getchat можно вызывать в группе: бот пришлёт chat_id в личку и постарается удалить команду в группе.

Требуется: python-telegram-bot>=20, pandas, openpyxl, python-dotenv
"""

import os
import re
import time
import html
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from telegram import Update, ReplyKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder, Application, ContextTypes,
    CommandHandler, MessageHandler, filters
)

# ---------- Конфиг ----------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Не найден BOT_TOKEN в .env!")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
XLSX_PATH = Path(os.getenv("FAQ_XLSX_PATH") or (DATA_DIR / "faq.xlsx"))

# Целевой чат для публикаций
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID")  # "-100..." или "@channelusername"
TARGET_CHAT_FILE = DATA_DIR / "target_chat.txt"

# Целевая тема (forum topic) в группе
TARGET_THREAD_ID: Optional[int] = None
TARGET_THREAD_FILE = DATA_DIR / "target_thread.txt"

# Админы публикаций
POST_ADMINS = {int(x) for x in (os.getenv("POST_ADMINS") or "").replace(" ", "").split(",") if x}

# Куда слать предложения
SUGGEST_CHAT_ID = os.getenv("SUGGEST_CHAT_ID")  # можно пусто — тогда в личку админам
SUGGEST_ADMINS = {int(x) for x in (os.getenv("SUGGEST_ADMINS") or "").replace(" ", "").split(",") if x} or POST_ADMINS

# Аудит активности (опц. уведомления)
AUDIT_CHAT_ID = os.getenv("AUDIT_CHAT_ID")  # можно пусто — тогда только CSV
AUDIT_CSV = DATA_DIR / "audit.csv"

# Лог предложений
SUGGESTIONS_CSV = DATA_DIR / "suggestions.csv"

# Особые листы Excel
SPECIAL_BCD_SHEETS = {"Доставка персонала (СДП)", "Подписание путевых листов"}

# Логи
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger("FAQBot")

# Ожидание вложений после /post
MAX_POST_WAIT_SEC = 180  # 3 минуты

# ---------- Кнопочные константы ----------
BTN_HELLO = "🐻 Поздороваться"
BTN_ASK   = "❓ У меня есть вопрос"
BTN_SUGG  = "💡 У меня есть предложение по модернизации данного бота"
BTN_HOWTO = "ℹ️ Как пользоваться ботом"
BTN_BACK  = "⬅️ Назад"

# ---------- Безопасность / лимиты ----------
STEM_SAFE = re.compile(r"^[\w\-\s\.]+$", re.IGNORECASE)
RATE_LIMIT = {"suggest_per_min": 2}
_last_suggest_at: Dict[int, List[float]] = {}

def _is_safe_stem(s: str) -> bool:
    return bool(STEM_SAFE.match(s or ""))

def _rate_limit_suggest(user_id: int) -> bool:
    now = time.time()
    window = 60
    bucket = _last_suggest_at.setdefault(user_id, [])
    while bucket and now - bucket[0] > window:
        bucket.pop(0)
    if len(bucket) >= RATE_LIMIT["suggest_per_min"]:
        return False
    bucket.append(now)
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

def _ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def _load_target_chat():
    global TARGET_CHAT_ID
    try:
        if TARGET_CHAT_FILE.exists():
            val = TARGET_CHAT_FILE.read_text(encoding="utf-8").strip()
            if val:
                TARGET_CHAT_ID = val
                logger.info("[CONFIG] Загружен TARGET_CHAT_ID из файла: %s", val)
    except Exception:
        logging.exception("Не удалось загрузить target_chat.txt")

def _save_target_chat(chat_id: str):
    try:
        _ensure_data_dir()
        TARGET_CHAT_FILE.write_text(chat_id, encoding="utf-8")
        logger.info("[CONFIG] Сохранён TARGET_CHAT_ID: %s", chat_id)
    except Exception:
        logging.exception("Не удалось сохранить target_chat.txt")

def _load_target_thread():
    global TARGET_THREAD_ID
    try:
        if TARGET_THREAD_FILE.exists():
            val = TARGET_THREAD_FILE.read_text(encoding="utf-8").strip()
            if val:
                TARGET_THREAD_ID = int(val)
                logger.info("[CONFIG] Загружен TARGET_THREAD_ID из файла: %s", val)
    except Exception:
        logging.exception("Не удалось загрузить target_thread.txt")

def _save_target_thread(thread_id: Optional[int]):
    try:
        _ensure_data_dir()
        if thread_id is None:
            if TARGET_THREAD_FILE.exists():
                TARGET_THREAD_FILE.unlink(missing_ok=True)
            logger.info("[CONFIG] Сброшен TARGET_THREAD_ID")
        else:
            TARGET_THREAD_FILE.write_text(str(thread_id), encoding="utf-8")
            logger.info("[CONFIG] Сохранён TARGET_THREAD_ID: %s", thread_id)
    except Exception:
        logging.exception("Не удалось сохранить target_thread.txt")

def _thread_kwargs():
    return {"message_thread_id": TARGET_THREAD_ID} if TARGET_THREAD_ID else {}

# ---------- Аудит ----------
def _audit_row(event: str, update: Optional[Update], details: str = "") -> List[str]:
    import datetime
    ts = datetime.datetime.now().isoformat(timespec="seconds")
    uid = update.effective_user.id if (update and update.effective_user) else ""
    uname = (f"@{update.effective_user.username}" if (update and update.effective_user and update.effective_user.username)
             else (update.effective_user.full_name if (update and update.effective_user) else ""))
    chat_id = update.effective_chat.id if (update and update.effective_chat) else ""
    chat_type = update.effective_chat.type if (update and update.effective_chat) else ""
    return [ts, str(uid), uname, str(chat_id), chat_type, event, details]

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
        logging.exception("Не удалось отправить аудит-уведомление")

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
    low = [c.lower() for c in cols]
    for kw in keywords:
        kw = kw.lower()
        for i, name in enumerate(low):
            if kw in name:
                return cols[i]
    return None

def _pick_many_columns(cols: List[str], keywords: List[str], exclude: Optional[str] = None) -> List[str]:
    found = []
    for col in cols:
        if exclude and col == exclude:
            continue
        name = col.lower()
        if any(kw.lower() in name for kw in keywords):
            found.append(col)
    return found

def _split_files_cell(val: str) -> List[str]:
    if not val:
        return []
    raw = re.split(r"[,\n;]+", val)
    return [s.strip() for s in raw if s.strip()]

def _split_post_stems(val: str) -> List[str]:
    if not val:
        return []
    raw = re.split(r"[,\n;]+", val)
    return [s.strip() for s in raw if s.strip()]

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
    out: List[Path] = []
    if s in FILE_INDEX:
        out.append(FILE_INDEX[s])
        return out
    for key, path in FILE_INDEX.items():
        if key.startswith(s):
            out.append(path)
    seen = set()
    uniq = []
    for p in out:
        if p not in seen:
            uniq.append(p); seen.add(p)
    return uniq

def _append_suggestion(chat_id: int, user_id: int, username: Optional[str], text: str):
    import csv, datetime
    _ensure_data_dir()
    is_new = not SUGGESTIONS_CSV.exists()
    with open(SUGGESTIONS_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        if is_new:
            w.writerow(["timestamp", "chat_id", "user_id", "username", "suggestion"])
        safe = _sanitize_for_csv(text)
        w.writerow([datetime.datetime.now().isoformat(timespec="seconds"), chat_id, user_id, username or "", safe])

async def _notify_about_suggestion(context: ContextTypes.DEFAULT_TYPE, text: str, from_user: str):
    safe_text = html.escape(text)
    msg = f"🆕 <b>Новое предложение</b>\nОт: {from_user}\n\n{safe_text}"
    delivered = False
    if SUGGEST_CHAT_ID:
        try:
            await context.bot.send_message(chat_id=SUGGEST_CHAT_ID, text=msg, parse_mode=ParseMode.HTML)
            delivered = True
            logger.info("[SUGGEST] в SUGGEST_CHAT_ID=%s", SUGGEST_CHAT_ID)
        except Exception:
            logging.exception("Не удалось отправить в SUGGEST_CHAT_ID=%s", SUGGEST_CHAT_ID)
    else:
        for uid in SUGGEST_ADMINS:
            try:
                await context.bot.send_message(chat_id=uid, text=msg, parse_mode=ParseMode.HTML)
                delivered = True
                logger.info("[SUGGEST] админу uid=%s", uid)
            except Exception:
                logging.exception("Не удалось написать админу %s", uid)
    if not delivered:
        logger.warning("[SUGGEST] Не доставлено ни одному получателю")

async def _send_answer_with_files(update: Update, html_text: str, files: Optional[List[str]]):
    await update.message.reply_html(html_text)
    if not files:
        return
    for stem in files:
        paths = _find_files_by_stem_fast(stem)
        if not paths:
            await update.message.reply_text(f"⚠️ Не найден файл: {stem}")
            continue
        file_path = paths[0]
        with open(file_path, "rb") as fh:
            await update.message.reply_document(document=fh, filename=file_path.name, caption=f"📎 {file_path.name}")

async def post_to_group(context: ContextTypes.DEFAULT_TYPE, text: str, files: Optional[List[Path]] = None):
    if not TARGET_CHAT_ID:
        raise RuntimeError("Не задан TARGET_CHAT_ID в .env и файле target_chat.txt")
    if files:
        first, *rest = files
        with open(first, "rb") as f:
            await context.bot.send_document(
                chat_id=TARGET_CHAT_ID, document=f, filename=first.name,
                caption=text, parse_mode=ParseMode.HTML, **_thread_kwargs()
            )
        for p in rest:
            with open(p, "rb") as f:
                await context.bot.send_document(chat_id=TARGET_CHAT_ID, document=f, filename=p.name, **_thread_kwargs())
    else:
        await context.bot.send_message(chat_id=TARGET_CHAT_ID, text=text, parse_mode=ParseMode.HTML, **_thread_kwargs())

# ---------- Репозиторий ----------
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
            df = df.fillna("")
            df = df[~(df.apply(lambda r: all((str(x).strip() == "" for x in r)), axis=1))].reset_index(drop=True)
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
        )
        if not answer_cols and len(cols) > 1:
            answer_cols = [cols[1]]
        items: List[FAQItem] = []
        for _, row in df.iterrows():
            q = _norm(str(row[q_col]))
            if not q:
                continue
            parts: List[str] = []
            for c in answer_cols:
                val = _norm(str(row[c]))
                if val and val.lower() != "nan":
                    if len(answer_cols) > 1:
                        parts.append(f"<b>{c}:</b> {val}")
                    else:
                        parts.append(val)
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
USER_FLOW: Dict[int, Optional[str]] = {}
POST_PENDING: Dict[int, Dict[str, object]] = {}  # chat_id -> {"desc": str, "stems": List[str], "ts": float}

# ---------- Клавиатуры ----------
def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [BTN_HELLO, BTN_ASK],
            [BTN_SUGG],
            [BTN_HOWTO]
        ],
        resize_keyboard=True
    )

def kb_categories() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[cat] for cat in CATEGORIES] + [[BTN_BACK]], resize_keyboard=True)

def kb_questions(category: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[it.question] for it in repo.data.get(category, [])] + [[BTN_BACK]], resize_keyboard=True)

# ---------- Вспомогательное: собрать вложения ----------
def _collect_attachments_from_message(update: Update) -> List[Dict[str, str]]:
    msg = update.message
    if not msg:
        return []
    collected: List[Dict[str, str]] = []
    if msg.document:
        collected.append({"type": "document", "file_id": msg.document.file_id, "filename": msg.document.file_name or ""})
    if msg.photo:
        largest = msg.photo[-1]
        collected.append({"type": "photo", "file_id": largest.file_id, "filename": ""})
    return collected

# ---------- Команды ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        await update.message.reply_text("Напиши мне в личку: открой профиль бота и нажми «Message».")
        return
    USER_CATEGORY[update.effective_chat.id] = None
    USER_FLOW[update.effective_chat.id] = None
    await update.message.reply_text(
        "Привет! Я <b>PtoShkinDSU_bot</b> 🤖\nВыбирай кнопку ниже 👇",
        reply_markup=kb_main(),
        parse_mode=ParseMode.HTML
    )
    await _audit("start", update, context, "Пользователь открыл бота")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        await update.message.reply_text("Эта команда доступна только в личке с ботом.")
        return
    await update.message.reply_text(
        "Команды: /post /send /publish /settarget /settopic /bindhere /deleteme /reindex /listfiles /myid /getchat\n\n"
        "Публикация в 2 шага:\n"
        "1) /post | Текст объявления\n"
        "2) Следом пришли файл(ы) Excel/PDF/картинку (до 3 минут)\n",
        reply_markup=kb_main()
    )
    await _audit("help", update, context, "Показ справки")

async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    uname = f"@{update.effective_user.username}" if update.effective_user.username else update.effective_user.full_name
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

# /deleteme — удаление с понятными причинами
async def deleteme(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from telegram.error import TelegramError
    import datetime

    chat = update.effective_chat
    msg = update.message

    # только группы/супергруппы
    if chat.type not in ("group", "supergroup"):
        if _is_private(update):
            await msg.reply_text("Эта команда работает только в группе.")
        return

    # что удаляем: ответ → цель; иначе → саму команду
    target = msg.reply_to_message or msg
    target_id = target.message_id

    # проверка прав бота
    try:
        me = await context.bot.get_me()
        my_member = await context.bot.get_chat_member(chat.id, me.id)
        status = getattr(my_member, "status", "")
        can_delete = False
        if status == "creator":
            can_delete = True
        elif status == "administrator":
            can_delete = bool(getattr(my_member, "can_delete_messages", False))
        if not can_delete:
            try:
                await msg.reply_text("Мне нужны права администратора с галочкой «Удалять сообщения».")
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

    # ограничение 48 часов
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        tdate = target.date
        if tdate.tzinfo is None:
            tdate = tdate.replace(tzinfo=datetime.timezone.utc)
        age_sec = (now - tdate).total_seconds()
        if age_sec > 48 * 3600:
            try:
                await msg.reply_text("Нельзя удалить: сообщению больше 48 часов.")
            except Exception:
                pass
            await _audit("deleteme_too_old", update, context, f"age_sec={int(age_sec)}")
            return
    except Exception as e:
        await _audit("deleteme_age_check_error", update, context, str(e))

    # удаление
    want_delete_command_too = bool(msg.reply_to_message)
    try:
        await context.bot.delete_message(chat_id=chat.id, message_id=target_id)
        if want_delete_command_too:
            try:
                await context.bot.delete_message(chat_id=chat.id, message_id=msg.message_id)
            except Exception:
                pass
        await _audit("deleteme_ok", update, context, f"deleted_msg_id={target_id}; also_cmd={want_delete_command_too}")
    except TelegramError as e:
        try:
            await msg.reply_text(f"❌ Не смог удалить: {e}")
        except Exception:
            pass
        await _audit("deleteme_error", update, context, str(e))

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
        await context.bot.send_message(chat_id=TARGET_CHAT_ID, text=payload, parse_mode=ParseMode.HTML, **_thread_kwargs())
        await update.message.reply_text("✅ Отправлено в группу.")
        await _audit("send", update, context, f"text_len={len(payload)}")
    except Exception as e:
        logging.exception("Ошибка отправки текста: %s", e)
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
        await context.bot.copy_message(
            chat_id=TARGET_CHAT_ID,
            from_chat_id=msg.chat.id,
            message_id=msg.reply_to_message.message_id,
            **_thread_kwargs()
        )
        await msg.reply_text("✅ Опубликовано в группу.")
        await _audit("publish", update, context, "copy_message")
    except Exception as e:
        logging.exception("Ошибка публикации копией: %s", e)
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

# ---------- FAQ / кнопки / предложения ----------
async def howto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        await update.message.reply_text("Эта функция доступна только в личке с ботом.")
        return
    guide = (
        "<b>Публикация в 2 шага</b>\n"
        "1) <code>/post | Текст объявления</code>\n"
        "2) В течение 3 минут пришли Excel/PDF/картинку\n\n"
        "Можно вместо вложений указать стемы файлов из <code>data/</code>:\n"
        "<code>/post отчет_октябрь | Сводка</code>\n"
        "Текст попадёт в подпись к первому файлу."
    )
    await update.message.reply_text(guide, parse_mode=ParseMode.HTML)
    await _audit("howto", update, context, "guide shown")

async def crab(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        return
    await update.message.reply_text("Привет, лови краба от моей медвежьей лапы! 🦀🐻")
    await _audit("button_hello", update, context, "crab")

async def ask_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        return
    USER_CATEGORY[update.effective_chat.id] = None
    await update.message.reply_text("Выбери категорию 👇", reply_markup=kb_categories())
    await _audit("button_ask", update, context, "open categories")

async def go_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        return
    chat_id = update.effective_chat.id
    if USER_CATEGORY.get(chat_id):
        USER_CATEGORY[chat_id] = None
        await update.message.reply_text("Категории 👇", reply_markup=kb_categories())
    else:
        USER_FLOW[chat_id] = None
        await update.message.reply_text("Главное меню 👇", reply_markup=kb_main())
    await _audit("button_back", update, context, "back")

async def choose_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        return
    cat = update.message.text
    if cat not in CATEGORIES:
        return
    USER_CATEGORY[update.effective_chat.id] = cat
    await update.message.reply_text(
        f"Категория: <b>{html.escape(cat)}</b>\nВыбери вопрос 👇",
        reply_markup=kb_questions(cat),
        parse_mode=ParseMode.HTML
    )
    await _audit("view_category", update, context, f"category={cat}")

async def choose_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        return
    chat_id = update.effective_chat.id
    cat = USER_CATEGORY.get(chat_id)
    if not cat:
        return
    q = update.message.text
    for it in repo.data.get(cat, []):
        if it.question == q:
            await _send_answer_with_files(update, it.render(), it.files)
            await _audit("view_question", update, context, f"category={cat}; question={q}")
            return

async def fuzzy_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        return
    text = (update.message.text or "").strip()
    from difflib import get_close_matches
    if not text:
        await update.message.reply_text("Не понял 🤔", reply_markup=kb_main())
        return
    chat_id = update.effective_chat.id
    current_cat = USER_CATEGORY.get(chat_id)
    if current_cat:
        options = [it.question for it in repo.data.get(current_cat, [])]
        match = get_close_matches(text, options, n=1, cutoff=0.5)
        if match:
            q = match[0]
            for it in repo.data[current_cat]:
                if it.question == q:
                    await _send_answer_with_files(update, f"🔎 Похоже, ты про:\n<b>{q}</b>\n\n{it.render()}", it.files)
                    await _audit("search_in_category", update, context, f"cat={current_cat}; query={text}; hit={q}")
                    return
        await update.message.reply_text("В этой категории не нашёл подходящего вопроса 🤔", reply_markup=kb_questions(current_cat))
        await _audit("search_in_category_nohit", update, context, f"cat={current_cat}; query={text}")
    else:
        if not ALL_QUESTIONS:
            await update.message.reply_text("База вопросов пуста. Проверь Excel.", reply_markup=kb_main())
            await _audit("search_empty_base", update, context, "no questions")
            return
        options = [q for (_, q) in ALL_QUESTIONS]
        match = get_close_matches(text, options, n=1, cutoff=0.5)
        if match:
            q = match[0]
            for cat, q_text in ALL_QUESTIONS:
                if q_text == q:
                    for it in repo.data.get(cat, []):
                        if it.question == q:
                            await update.message.reply_html(f"🔎 Ближе всего:\n<b>{q}</b>\n<i>Категория: {cat}</i>")
                            await _send_answer_with_files(update, it.render(), it.files)
                            await _audit("search_global", update, context, f"query={text}; hit_cat={cat}; hit_q={q}")
                            return
        await update.message.reply_text("Не нашёл подходящего ответа 🤔", reply_markup=kb_categories())
        await _audit("search_global_nohit", update, context, f"query={text}")

# ---------- /post ----------
async def cmd_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        await update.message.reply_text("Эта команда доступна только в личке с ботом.")
        return
    uid = update.effective_user.id if update.effective_user else 0
    if not _is_post_admin(uid):
        await update.message.reply_text("⛔ У тебя нет прав на публикацию.")
        return

    raw_all = (update.message.text or update.message.caption or "").strip()
    if raw_all.lower().startswith("/post"):
        after = raw_all.split(" ", 1)[1] if " " in raw_all else ""
    else:
        after = raw_all

    if "|" in after:
        stem_part, desc_part = after.split("|", 1)
        stems = _split_post_stems(stem_part.strip())
        desc = desc_part.strip()
    else:
        stems = []
        desc = after.strip()

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
        if matched:
            files_from_data.append(matched[0])
        else:
            missing.append(stem)
    if missing:
        await update.message.reply_text("⚠️ Не найдены файлы: " + ", ".join(missing))

    try:
        sent_any = False

        if attachments:
            for i, att in enumerate(attachments):
                cap = desc if (i == 0 and desc) else None
                if att["type"] == "document":
                    await context.bot.send_document(
                        chat_id=TARGET_CHAT_ID,
                        document=att["file_id"],
                        caption=cap,
                        parse_mode=ParseMode.HTML if cap else None,
                        **_thread_kwargs()
                    )
                elif att["type"] == "photo":
                    await context.bot.send_photo(
                        chat_id=TARGET_CHAT_ID,
                        photo=att["file_id"],
                        caption=cap,
                        parse_mode=ParseMode.HTML if cap else None,
                        **_thread_kwargs()
                    )
            sent_any = True

        if files_from_data:
            if not sent_any and desc:
                first, *rest = files_from_data
                with open(first, "rb") as f:
                    await context.bot.send_document(
                        chat_id=TARGET_CHAT_ID,
                        document=f,
                        filename=first.name,
                        caption=desc,
                        parse_mode=ParseMode.HTML,
                        **_thread_kwargs()
                    )
                for p in rest:
                    with open(p, "rb") as f:
                        await context.bot.send_document(chat_id=TARGET_CHAT_ID, document=f, filename=p.name, **_thread_kwargs())
            else:
                for p in files_from_data:
                    with open(p, "rb") as f:
                        await context.bot.send_document(chat_id=TARGET_CHAT_ID, document=f, filename=p.name, **_thread_kwargs())
            sent_any = True

        if not sent_any:
            if desc:
                await context.bot.send_message(chat_id=TARGET_CHAT_ID, text=desc, parse_mode=ParseMode.HTML, **_thread_kwargs())
                sent_any = True
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
        logging.exception("Ошибка публикации: %s", e)
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

# ---------- Предложения ----------
async def suggest_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        return
    chat_id = update.effective_chat.id
    USER_FLOW[chat_id] = "suggest"
    await update.message.reply_text(
        "Напиши, пожалуйста, своё предложение одним сообщением.\n"
        "Можно приложить ссылки/описания. После отправки я всё перекину админу. ✍️",
        reply_markup=ReplyKeyboardMarkup([[BTN_BACK]], resize_keyboard=True)
    )
    await _audit("suggest_start", update, context, "start")

async def suggest_capture(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_private(update):
        return
    chat_id = update.effective_chat.id
    if USER_FLOW.get(chat_id) != "suggest":
        return
    text = (update.message.text or "").strip()

    uid = update.effective_user.id if update.effective_user else 0
    if not _rate_limit_suggest(uid):
        await update.message.reply_text("Слишком часто. Попробуй чуть позже 🙏")
        await _audit("suggest_ratelimit", update, context, "too many")
        return

    if text in {BTN_HELLO, BTN_ASK, BTN_SUGG, BTN_HOWTO}:
        return
    if not text or text == BTN_BACK:
        USER_FLOW[chat_id] = None
        await update.message.reply_text("Отменил ввод предложения. Возвращаю в меню 👇", reply_markup=kb_main())
        await _audit("suggest_cancel", update, context, "cancel")
        return

    user = update.effective_user
    username = f"@{user.username}" if (user and user.username) else (user.full_name if user else "user")
    _append_suggestion(chat_id, user.id if user else 0, user.username if user else "", text)
    await _notify_about_suggestion(context, text, username)
    USER_FLOW[chat_id] = None
    await update.message.reply_text("Спасибо! 🚀 Твоё предложение отправлено админам. Возвращаю в меню 👇", reply_markup=kb_main())
    await _audit("suggest_sent", update, context, f"len={len(text)}")

# --- Хук старта ---
async def _on_start(app: Application):
    _build_file_index()
    _load_target_chat()
    _load_target_thread()
    await app.bot.delete_webhook(drop_pending_updates=True)
    me = await app.bot.get_me()
    logger.info("Bot started as @%s (id=%s), target=%s, thread=%s", me.username, me.id, TARGET_CHAT_ID, TARGET_THREAD_ID)

# ---------- Роутинг ----------
def build_app() -> Application:
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(_on_start).build()

    # Команды
    app.add_handler(CommandHandler("getchat", getchat), group=0)
    app.add_handler(CommandHandler("start", start, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("help", help_cmd, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("listfiles", listfiles, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("myid", myid), group=0)  # можно и в группе
    app.add_handler(CommandHandler("post", cmd_post, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("send", send_text, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("publish", publish_reply, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("reindex", cmd_reindex, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("settarget", settarget, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("settopic", settopic, filters=filters.ChatType.PRIVATE), group=0)
    app.add_handler(CommandHandler("bindhere", bindhere), group=0)  # вызывать в нужной теме группы
    app.add_handler(CommandHandler("deleteme", deleteme), group=0)  # удаление сообщения в группе

    # Кнопки — только в ЛС
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & filters.TEXT & filters.Regex(r"^🐻 (Поздороваться|Лови краба)$"),
        crab
    ), group=1)
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & filters.TEXT & filters.Regex(rf"^{re.escape(BTN_ASK)}$"),
        ask_category
    ), group=1)
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & filters.TEXT & filters.Regex(rf"^{re.escape(BTN_SUGG)}$"),
        suggest_start
    ), group=1)
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & filters.TEXT & filters.Regex(rf"^{re.escape(BTN_HOWTO)}$"),
        howto
    ), group=1)
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & filters.TEXT & filters.Regex(rf"^{re.escape(BTN_BACK)}$"),
        go_back
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

    # Категории/вопросы/fuzzy — только в ЛС
    if CATEGORIES:
        pattern = r"^(" + "|".join(map(re.escape, CATEGORIES)) + r")$"
        app.add_handler(MessageHandler(
            filters.ChatType.PRIVATE & filters.TEXT & filters.Regex(pattern),
            choose_category
        ), group=3)

    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND,
        choose_question,
        block=False
    ), group=4)
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND,
        fuzzy_search
    ), group=4)

    return app

# ---------- Точка входа ----------
if __name__ == "__main__":
    print(f"[DEBUG] BASE_DIR: {BASE_DIR}")
    print(f"[DEBUG] XLSX_PATH: {XLSX_PATH} (exists={XLSX_PATH.exists()})")
    app = build_app()
    print("Bot is starting…")

    # NEW: Автоматический выбор режима. Если задан BASE_URL — запускаем webhook (для Render/Glitch/Koyeb).
    BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
    port = int(os.getenv("PORT", "8000"))

    if BASE_URL:
        webhook_path = f"/{BOT_TOKEN}"
        full_url = f"{BASE_URL}{webhook_path}"
        print(f"[DEBUG] Using WEBHOOK at {full_url} (port={port})")
        app.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=BOT_TOKEN,       # секретная часть URL (никому не показывать)
            webhook_url=full_url,      # полный публичный URL https://<host>/<BOT_TOKEN>
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