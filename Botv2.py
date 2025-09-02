#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Botv3_secure_post_wait.py — Telegram-бот (python-telegram-bot v20+)

Главное в этой версии:
- /post понимает медиа в том же сообщении (caption) И в следующем сообщении (ожидание вложений до 3 минут).
- Поддержаны: документы (Excel/PDF/любые), фото (одно и альбом).
- Сохранил все прошлые меры безопасности: html.escape, CSV-защита, /publish «только своё», антиспам предложений, валидация стемов.

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

TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID")  # "-100..." или "@channelusername"
POST_ADMINS = {int(x) for x in (os.getenv("POST_ADMINS") or "").replace(" ", "").split(",") if x}

SUGGEST_CHAT_ID = os.getenv("SUGGEST_CHAT_ID")
SUGGEST_ADMINS = {int(x) for x in (os.getenv("SUGGEST_ADMINS") or "").replace(" ", "").split(",") if x} or POST_ADMINS

SUGGESTIONS_CSV = DATA_DIR / "suggestions.csv"

SPECIAL_BCD_SHEETS = {"Доставка персонала (СДП)", "Подписание путевых листов"}

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger("FAQBot")

# ---------- Кнопочные константы ----------
BTN_HELLO = "🐻 Поздороваться"
BTN_ASK   = "❓ У меня есть вопрос"
BTN_SUGG  = "💡 У меня есть предложение по модернизации данного бота"
BTN_HOWTO = "ℹ️ Как пользоваться ботом"
BTN_BACK  = "⬅️ Назад"

# ---------- Безопасность: лимиты / стемы / антиспам ----------
STEM_SAFE = re.compile(r"^[\w\-\s\.]+$", re.IGNORECASE)
RATE_LIMIT = {"suggest_per_min": 2}
_last_suggest_at: Dict[int, List[float]] = {}
MAX_POST_WAIT_SEC = 180  # 3 минуты ожидания вложений после /post

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

# ---------- Утилиты ----------
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

def _is_post_admin(user_id: int) -> bool:
    return user_id in POST_ADMINS

def _ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

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
            logger.info("[SUGGEST] отправлено в SUGGEST_CHAT_ID=%s", SUGGEST_CHAT_ID)
        except Exception:
            logging.exception("Не удалось отправить в SUGGEST_CHAT_ID=%s", SUGGEST_CHAT_ID)
    else:
        for uid in SUGGEST_ADMINS:
            try:
                await context.bot.send_message(chat_id=uid, text=msg, parse_mode=ParseMode.HTML)
                delivered = True
                logger.info("[SUGGEST] отправлено админу uid=%s", uid)
            except Exception:
                logging.exception("Не удалось написать админу %s (возможно, он не писал боту в личку)", uid)
    if not delivered:
        logger.warning("[SUGGEST] Не удалось доставить предложение ни одному получателю")

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
        raise RuntimeError("Не задан TARGET_CHAT_ID в .env")
    if files:
        first, *rest = files
        with open(first, "rb") as f:
            await context.bot.send_document(
                chat_id=TARGET_CHAT_ID, document=f, filename=first.name,
                caption=text, parse_mode=ParseMode.HTML
            )
        for p in rest:
            with open(p, "rb") as f:
                await context.bot.send_document(chat_id=TARGET_CHAT_ID, document=f, filename=p.name)
    else:
        await context.bot.send_message(chat_id=TARGET_CHAT_ID, text=text, parse_mode=ParseMode.HTML)

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

# ---------- Вспомогательное: сбор вложений из Message ----------
def _collect_attachments_from_message(update: Update) -> List[Dict[str, str]]:
    """Возвращает список вложений в виде словарей: {"type": "document|photo", "file_id": str, "filename": Optional[str]}"""
    msg = update.message
    if not msg:
        return []
    collected: List[Dict[str, str]] = []

    # Документ (Excel/PDF/любые файлы)
    if msg.document:
        collected.append({
            "type": "document",
            "file_id": msg.document.file_id,
            "filename": msg.document.file_name or ""
        })

    # Фото (одно)
    if msg.photo:
        # Берём самое большое (последний размер)
        largest = msg.photo[-1]
        collected.append({
            "type": "photo",
            "file_id": largest.file_id,
            "filename": ""
        })

    # Альбом фото (photo в альбоме тоже приходит как photo, Telegram сам разобьёт по сообщениям с одинаковым media_group_id)
    # Для простоты — каждый элемент обрабатывается отдельно, логика та же.

    return collected

# ---------- Хендлеры ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    USER_CATEGORY[update.effective_chat.id] = None
    USER_FLOW[update.effective_chat.id] = None
    await update.message.reply_text(
        "Привет! Я <b>PtoShkinDSU_bot</b> 🤖\nВыбирай кнопку ниже 👇",
        reply_markup=kb_main(),
        parse_mode=ParseMode.HTML
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Команды: /start /help /getchat /listfiles /myid /post /send /publish /reindex\n"
        "• /post <стем[,стем2]> | <описание> — файлы из data/ и/или прикреплённый файл (Excel/PDF/картинка)\n"
        "• Можно отправить /post, а затем ДОБАВИТЬ файл отдельным сообщением — я подожду 3 минуты\n"
        "• /publish — ответом на своё сообщение с медиа (скопирует «как есть»)\n"
        "• /send <текст> — быстро отправить текст\n",
        reply_markup=kb_main()
    )

async def howto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    guide = (
        "<b>Как пользоваться ботом</b>\n\n"
        "📤 <b>Публикация</b>\n"
        "• <code>/post | Текст</code> — только текст\n"
        "• <code>/post ФАЙЛ | Текст</code> — файл из <code>data/</code> по стему и подпись\n"
        "• Прикрепи файл прямо к сообщению с <code>/post</code> — уйдёт в группу\n"
        "• Или отправь файл <i>следом</i> в течение 3 минут — тоже опубликую\n\n"
        "🆔 <b>Техничка</b>: /myid, /getchat, /listfiles, /reindex\n"
    )
    await update.message.reply_text(guide, parse_mode=ParseMode.HTML)

async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    uname = f"@{update.effective_user.username}" if update.effective_user.username else update.effective_user.full_name
    await update.message.reply_text(f"👤 Твой Telegram ID: {uid}\nИмя: {uname}")

async def crab(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Привет, лови краба от моей медвежьей лапы! 🦀🐻")

async def ask_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    USER_CATEGORY[update.effective_chat.id] = None
    await update.message.reply_text("Выбери категорию 👇", reply_markup=kb_categories())

async def go_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if USER_CATEGORY.get(chat_id):
        USER_CATEGORY[chat_id] = None
        await update.message.reply_text("Категории 👇", reply_markup=kb_categories())
    else:
        USER_FLOW[chat_id] = None
        await update.message.reply_text("Главное меню 👇", reply_markup=kb_main())

async def choose_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cat = update.message.text
    if cat not in CATEGORIES:
        return
    USER_CATEGORY[update.effective_chat.id] = cat
    await update.message.reply_text(
        f"Категория: <b>{cat}</b>\nВыбери вопрос 👇",
        reply_markup=kb_questions(cat),
        parse_mode=ParseMode.HTML
    )

async def choose_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cat = USER_CATEGORY.get(chat_id)
    if not cat:
        return
    q = update.message.text
    for it in repo.data.get(cat, []):
        if it.question == q:
            await _send_answer_with_files(update, it.render(), it.files)
            return

# Fuzzy-поиск
async def fuzzy_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if not text:
        await update.message.reply_text("Не понял 🤔", reply_markup=kb_main())
        return
    from difflib import get_close_matches
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
                    return
        await update.message.reply_text("В этой категории не нашёл подходящего вопроса 🤔", reply_markup=kb_questions(current_cat))
    else:
        if not ALL_QUESTIONS:
            await update.message.reply_text("База вопросов пуста. Проверь Excel.", reply_markup=kb_main())
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
                            return
        await update.message.reply_text("Не нашёл подходящего ответа 🤔", reply_markup=kb_categories())

# ---------- /post (улучшенный: caption + ожидание вложений) ----------
async def cmd_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    uid = update.effective_user.id if update.effective_user else 0
    if not _is_post_admin(uid):
        await update.message.reply_text("⛔ У тебя нет прав на публикацию.")
        return

    # 1) Текст команды из text или caption
    raw_all = (update.message.text or update.message.caption or "").strip()

    # 2) Снимаем префикс '/post'
    if raw_all.lower().startswith("/post"):
        after = raw_all.split(" ", 1)[1] if " " in raw_all else ""
    else:
        after = raw_all

    # 3) Разбор стемов/описания
    if "|" in after:
        stem_part, desc_part = after.split("|", 1)
        stems = _split_post_stems(stem_part.strip())
        desc = desc_part.strip()
    else:
        stems = []
        desc = after.strip()

    # 4) Собираем вложения из этого сообщения (если есть)
    attachments = _collect_attachments_from_message(update)

    # 5) Если вложений нет — запомним ожидание вложений до 3 минут
    if not attachments:
        POST_PENDING[update.effective_chat.id] = {"desc": desc, "stems": stems, "ts": time.time()}
        if stems:
            await update.message.reply_text(
                "Принято. Можешь прислать файл(ы) (Excel/PDF/фото) отдельным сообщением — опубликую вместе с описанием.\n"
                "Если не пришлёшь — опубликую только текст/файлы из data/."
            )
        else:
            await update.message.reply_text(
                "Принято. Жду файл(ы) (Excel/PDF/фото) отдельным сообщением в течение 3 минут — опубликую с этим описанием.\n"
                "Или повтори /post с указанием стемов для файлов из data/."
            )

    # 6) Публикация (вложения из текущего сообщения + файлы из data/ по стемам)
    await _do_publish(update, context, desc, stems, attachments)

async def _do_publish(update: Update, context: ContextTypes.DEFAULT_TYPE, desc: str, stems: List[str], attachments: List[Dict[str, str]]):
    # Файлы из data/ по стемам
    files_from_data: List[Path] = []
    missing: List[str] = []
    for stem in stems:
        if not _is_safe_stem(stem):
            await update.message.reply_text(f"Недопустимое имя файла: {stem}")
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

        # 1) Вложения из сообщения (документы/фото)
        if attachments:
            for i, att in enumerate(attachments):
                cap = desc if (i == 0 and desc) else None
                if att["type"] == "document":
                    await context.bot.send_document(
                        chat_id=TARGET_CHAT_ID,
                        document=att["file_id"],
                        caption=cap,
                        parse_mode=ParseMode.HTML if cap else None
                    )
                elif att["type"] == "photo":
                    await context.bot.send_photo(
                        chat_id=TARGET_CHAT_ID,
                        photo=att["file_id"],
                        caption=cap,
                        parse_mode=ParseMode.HTML if cap else None
                    )
            sent_any = True

        # 2) Файлы из data/
        if files_from_data:
            if not sent_any and desc:
                first, *rest = files_from_data
                with open(first, "rb") as f:
                    await context.bot.send_document(
                        chat_id=TARGET_CHAT_ID,
                        document=f,
                        filename=first.name,
                        caption=desc,
                        parse_mode=ParseMode.HTML
                    )
                for p in rest:
                    with open(p, "rb") as f:
                        await context.bot.send_document(chat_id=TARGET_CHAT_ID, document=f, filename=p.name)
            else:
                for p in files_from_data:
                    with open(p, "rb") as f:
                        await context.bot.send_document(chat_id=TARGET_CHAT_ID, document=f, filename=p.name)
            sent_any = True

        # 3) Если вообще ничего не отправили — отправим только текст (если он есть)
        if not sent_any:
            if desc:
                await context.bot.send_message(chat_id=TARGET_CHAT_ID, text=desc, parse_mode=ParseMode.HTML)
                sent_any = True
            else:
                await update.message.reply_text(
                    "Нечего публиковать: ни вложений, ни стемов, ни описания.\n"
                    "Использование: /post <стем[,стем2]> | <описание> — или прикрепи файл(ы)."
                )
                return

        await update.message.reply_text("✅ Опубликовано.")
    except Exception as e:
        logging.exception("Ошибка публикации: %s", e)
        await update.message.reply_text(f"❌ Ошибка публикации: {e}")

# ---------- Приём вложений ПОСЛЕ команды /post ----------
async def capture_post_attachments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Если пользователь недавно отправил /post без вложений — ловим следующее сообщение с документом/фото и публикуем."""
    chat_id = update.effective_chat.id
    pending = POST_PENDING.get(chat_id)
    if not pending:
        return
    # Проверим TTL ожидания
    if time.time() - float(pending.get("ts", 0)) > MAX_POST_WAIT_SEC:
        POST_PENDING.pop(chat_id, None)
        return

    # Соберём вложения из текущего сообщения
    atts = _collect_attachments_from_message(update)
    if not atts:
        return  # это не медиа — ничего не делаем

    desc = str(pending.get("desc") or "")
    stems = list(pending.get("stems") or [])
    # Очистим ожидание, чтобы не публиковать повторно
    POST_PENDING.pop(chat_id, None)

    await _do_publish(update, context, desc, stems, atts)

# ---------- Предложения ----------
async def suggest_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    USER_FLOW[chat_id] = "suggest"
    await update.message.reply_text(
        "Напиши, пожалуйста, своё предложение одним сообщением.\n"
        "Можно приложить ссылки/описания. После отправки я всё перекину админу. ✍️",
        reply_markup=ReplyKeyboardMarkup([[BTN_BACK]], resize_keyboard=True)
    )

async def suggest_capture(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if USER_FLOW.get(chat_id) != "suggest":
        return
    text = (update.message.text or "").strip()

    uid = update.effective_user.id if update.effective_user else 0
    if not _rate_limit_suggest(uid):
        await update.message.reply_text("Слишком часто. Попробуй чуть позже 🙏")
        return

    if text in {BTN_HELLO, BTN_ASK, BTN_SUGG, BTN_HOWTO}:
        return
    if not text or text == BTN_BACK:
        USER_FLOW[chat_id] = None
        await update.message.reply_text("Отменил ввод предложения. Возвращаю в меню 👇", reply_markup=kb_main())
        return

    user = update.effective_user
    username = f"@{user.username}" if (user and user.username) else (user.full_name if user else "user")
    _append_suggestion(chat_id, user.id if user else 0, user.username if user else "", text)
    await _notify_about_suggestion(context, text, username)
    USER_FLOW[chat_id] = None
    await update.message.reply_text("Спасибо! 🚀 Твоё предложение отправлено админам. Возвращаю в меню 👇", reply_markup=kb_main())

# ---------- Прочие команды ----------
async def getchat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    await update.message.reply_text(f"chat_id = {chat.id}")

async def listfiles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not DATA_DIR.exists():
        await update.message.reply_text("Папка data/ не найдена.")
        return
    files = [p.name for p in DATA_DIR.iterdir() if p.is_file()]
    if not files:
        await update.message.reply_text("В папке data/ файлов нет.")
    else:
        msg = "📂 Файлы в data/:\n" + "\n".join(f"• {f}" for f in files)
        await update.message.reply_text(msg)

async def send_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.eective_user.id if update.effective_user else 0
    if not _is_post_admin(user_id):
        await update.message.reply_text("⛔ У тебя нет прав на публикацию.")
        return
    raw = (update.message.text or "").strip()
    payload = raw.split(" ", 1)[1].strip() if " " in raw else ""
    if not payload:
        await update.message.reply_text("Использование: /send <текст сообщения>")
        return
    try:
        await context.bot.send_message(chat_id=TARGET_CHAT_ID, text=payload, parse_mode=ParseMode.HTML)
        await update.message.reply_text("✅ Отправлено в группу.")
    except Exception as e:
        logging.exception("Ошибка отправки текста: %s", e)
        await update.message.reply_text(f"❌ Ошибка отправки: {e}")

async def publish_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else 0
    if not _is_post_admin(user_id):
        await update.message.reply_text("⛔ У тебя нет прав на публикацию.")
        return
    msg = update.message
    if not msg.reply_to_message:
        await msg.reply_text("Нужно ответить этой командой на сообщение (с текстом/медиа), которое надо опубликовать.")
        return
    if not msg.reply_to_message.from_user or msg.reply_to_message.from_user.id != user_id:
        await msg.reply_text("Можно публиковать только собственные сообщения.")
        return
    try:
        await context.bot.copy_message(
            chat_id=TARGET_CHAT_ID,
            from_chat_id=msg.chat.id,
            message_id=msg.reply_to_message.message_id
        )
        await msg.reply_text("✅ Опубликовано в группу.")
    except Exception as e:
        logging.exception("Ошибка публикации копией: %s", e)
        await msg.reply_text(f"❌ Ошибка публикации: {e}")

# --- Хук старта ---
async def _on_start(app: Application):
    _build_file_index()
    await app.bot.delete_webhook(drop_pending_updates=True)
    me = await app.bot.get_me()
    logger.info("Bot started as @%s (id=%s)", me.username, me.id)

async def cmd_reindex(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not u or u.id not in POST_ADMINS:
        await update.message.reply_text("⛔ Нет прав.")
        return
    _build_file_index()
    await update.message.reply_text("🔄 Индекс файлов пересобран.")

# ---------- Роутинг ----------
def build_app() -> Application:
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(_on_start).build()

    # Команды
    app.add_handler(CommandHandler("start", start), group=0)
    app.add_handler(CommandHandler("help", help_cmd), group=0)
    app.add_handler(CommandHandler("getchat", getchat), group=0)
    app.add_handler(CommandHandler("listfiles", listfiles), group=0)
    app.add_handler(CommandHandler("myid", myid), group=0)
    app.add_handler(CommandHandler("post", cmd_post), group=0)
    app.add_handler(CommandHandler("send", send_text), group=0)
    app.add_handler(CommandHandler("publish", publish_reply), group=0)
    app.add_handler(CommandHandler("reindex", cmd_reindex), group=0)

    # Кнопки
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"^🐻 (Поздороваться|Лови краба)$"), crab), group=1)
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(rf"^{re.escape(BTN_ASK)}$"), ask_category), group=1)
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(rf"^{re.escape(BTN_SUGG)}$"), suggest_start), group=1)
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(rf"^{re.escape(BTN_HOWTO)}$"), howto), group=1)
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(rf"^{re.escape(BTN_BACK)}$"), go_back), group=1)

    # Ловим вложения после /post: документы и фото (включая альбомы)
    app.add_handler(MessageHandler((filters.Document.ALL | filters.PHOTO) & ~filters.COMMAND, capture_post_attachments), group=1)

    # Предложения — раньше общих текстов
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, suggest_capture), group=2)

    # Категории
    if CATEGORIES:
        pattern = r"^(" + "|".join(map(re.escape, CATEGORIES)) + r")$"
        app.add_handler(MessageHandler(filters.TEXT & filters.Regex(pattern), choose_category), group=3)

    # Вопросы + fuzzy
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, choose_question, block=False), group=4)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, fuzzy_search), group=4)

    return app

# ---------- Точка входа ----------
if __name__ == "__main__":
    print(f"[DEBUG] BASE_DIR: {BASE_DIR}")
    print(f"[DEBUG] XLSX_PATH: {XLSX_PATH} (exists={XLSX_PATH.exists()})")
    app = build_app()
    print("Bot is running…")
    app.run_polling(
        close_loop=False,
        drop_pending_updates=True,
        allowed_updates=["message"],
        stop_signals=None,
    )