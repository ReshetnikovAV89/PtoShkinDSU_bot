#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Botv2.py — Telegram-бот (python-telegram-bot v20+)

Функции:
- FAQ из Excel (data/faq.xlsx): каждая вкладка = категория, вопросы/ответы парсятся автоматически.
- Особые вкладки: "Доставка персонала (СДП)", "Подписание путевых листов" — A(Вопрос), B(Ответ1), C(Ответ2), D(Комментарий).
- Колонка "Файл" (или File/Files) в Excel: имена/стемы документов из data/ — бот прикрепит их к ответу.
- Кнопки: 🐻 Лови краба, ❓ У меня есть вопрос, 💡 У меня есть предложение...
- Ввод предложений + уведомление админам.
- Команда /post — публикация поста в канал/группу (в т.ч. несколько файлов).

Требуется: python-telegram-bot>=20, pandas, openpyxl, python-dotenv
"""

import os
import re
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

# Публикации в канал/группу:
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID")  # например "-1001234567890" или "@channelusername"
POST_ADMINS = {int(x) for x in (os.getenv("POST_ADMINS") or "").replace(" ", "").split(",") if x}

# Предложения (уведомления):
SUGGEST_CHAT_ID = os.getenv("SUGGEST_CHAT_ID")  # можно пусто — тогда шлём админам
SUGGEST_ADMINS = {int(x) for x in (os.getenv("SUGGEST_ADMINS") or "").replace(" ", "").split(",") if x} or POST_ADMINS

# Файл с предложениями (лог):
SUGGESTIONS_CSV = DATA_DIR / "suggestions.csv"

# Вкладки, где берём строго A(вопрос), B(Ответ1), C(Ответ2), D(Комментарий)
SPECIAL_BCD_SHEETS = {
    "Доставка персонала (СДП)",
    "Подписание путевых листов",
}

# Логи на INFO (быстрее, чем DEBUG)
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger("FAQBot")

# ---------- Модель ----------
@dataclass
class FAQItem:
    question: str
    answer: Optional[str] = None
    answer_1: Optional[str] = None
    answer_2: Optional[str] = None
    comment: Optional[str] = None
    files: Optional[List[str]] = None  # стемы файлов из колонки "Файл"

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
    """Разбивает значение ячейки 'Файл' на список стемов (поддержка , ; и переноса строки)."""
    if not val:
        return []
    raw = re.split(r"[,\n;]+", val)
    return [s.strip() for s in raw if s.strip()]

def _split_post_stems(val: str) -> List[str]:
    """Разбивает список стемов в /post: запятая, ;, перенос строки."""
    if not val:
        return []
    raw = re.split(r"[,\n;]+", val)
    return [s.strip() for s in raw if s.strip()]

# --- Индекс файлов для быстрых поисков ---
FILE_INDEX: Dict[str, Path] = {}

def _build_file_index():
    """Индексируем файлы из data/: ключи — stem и полное имя (lower)."""
    FILE_INDEX.clear()
    if DATA_DIR.exists():
        for p in DATA_DIR.iterdir():
            if p.is_file():
                FILE_INDEX[p.stem.lower()] = p
                FILE_INDEX[p.name.lower()] = p

def _find_files_by_stem_fast(stem: str) -> List[Path]:
    """Сначала точное совпадение по индексу, затем префиксный поиск."""
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
    # уникализируем, сохраняя порядок
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
    """Пишем предложение в CSV."""
    import csv, datetime
    _ensure_data_dir()
    is_new = not SUGGESTIONS_CSV.exists()
    with open(SUGGESTIONS_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        if is_new:
            w.writerow(["timestamp", "chat_id", "user_id", "username", "suggestion"])
        w.writerow([datetime.datetime.now().isoformat(timespec="seconds"), chat_id, user_id, username or "", text])

async def _notify_about_suggestion(context: ContextTypes.DEFAULT_TYPE, text: str, from_user: str):
    """Уведомления о предложениях (в общий чат или лички админам)."""
    msg = f"🆕 <b>Новое предложение</b>\nОт: {from_user}\n\n{text}"
    if SUGGEST_CHAT_ID:
        await context.bot.send_message(chat_id=SUGGEST_CHAT_ID, text=msg, parse_mode=ParseMode.HTML)
    else:
        for uid in SUGGEST_ADMINS:
            try:
                await context.bot.send_message(chat_id=uid, text=msg, parse_mode=ParseMode.HTML)
            except Exception:
                logging.exception("Не удалось отправить уведомление админу %s", uid)

async def _send_answer_with_files(update: Update, html_text: str, files: Optional[List[str]]):
    """Сначала текст, затем — найденные файлы по списку стемов."""
    await update.message.reply_html(html_text)
    if not files:
        return
    for stem in files:
        paths = _find_files_by_stem_fast(stem)
        if not paths:
            logging.warning("Файл по стему '%s' не найден в %s", stem, DATA_DIR)
            await update.message.reply_text(f"⚠️ Не найден файл: {stem}")
            continue
        file_path = paths[0]
        try:
            with open(file_path, "rb") as fh:
                await update.message.reply_document(document=fh, filename=file_path.name, caption=f"📎 {file_path.name}")
        except Exception as e:
            logging.exception("Ошибка отправки файла %s: %s", file_path, e)
            await update.message.reply_text(f"⚠️ Не удалось отправить файл: {file_path.name}")

async def post_to_group(context: ContextTypes.DEFAULT_TYPE, text: str, files: Optional[List[Path]] = None):
    """Пост в канал/группу TARGET_CHAT_ID: текст + (необязательно) один или несколько файлов."""
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
    """Читает Excel и строит структуру: {категория: [FAQItem, ...]}"""

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
            # убрать полностью пустые строки
            df = df[~(df.apply(lambda r: all((str(x).strip() == "" for x in r)), axis=1))].reset_index(drop=True)

            if sheet.strip() in SPECIAL_BCD_SHEETS:
                items = self._parse_special_bcd(df)
            else:
                items = self._parse_generic(df)

            items = [it for it in items if it.question and it.question.strip()]
            if items:
                normalized[sheet] = items
                logger.info("Категория '%s': %d вопросов", sheet, len(items))

        if not normalized:
            raise RuntimeError("Не удалось извлечь FAQ ни с одной вкладки Excel.")
        self.data = normalized
        logger.info("FAQ загружен: %d вкладок", len(self.data))

    def _extract_files_from_row(self, df: pd.DataFrame, row: pd.Series) -> List[str]:
        """Ищем колонку 'Файл'/'File'/'Files' и возвращаем список стемов из ячейки."""
        file_cols = [c for c in df.columns if ("файл" in c.lower()) or ("file" in c.lower())]
        if not file_cols:
            return []
        cell = _norm(str(row[file_cols[0]]))
        if not cell or cell.lower() == "nan":
            return []
        return _split_files_cell(cell)

    def _parse_special_bcd(self, df: pd.DataFrame) -> List[FAQItem]:
        """A=вопрос, B=Ответ1, C=Ответ2, D=Комментарий; + опциональная колонка 'Файл'."""
        if df.shape[1] < 4:
            logger.warning("Особая вкладка имеет меньше 4 колонок — пропускаю.")
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
        """
        Общая логика:
        - Вопрос: по ключам ("вопрос","тема","наименование","заголовок") или первая колонка.
        - Ответ(ы): по ключам ("ответ","описание","информация","что делать","как","где","контакт","телефон",
          "email","почта","ссылка","адрес","комментар"). Если ответов несколько — склеиваем их с подписями.
        - Колонка 'Файл' (или 'File/Files') — список стемов через запятую/; или перенос строки.
        """
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

            # текстовый ответ
            parts: List[str] = []
            for c in answer_cols:
                val = _norm(str(row[c]))
                if val and val.lower() != "nan":
                    if len(answer_cols) > 1:
                        parts.append(f"<b>{c}:</b> {val}")
                    else:
                        parts.append(val)
            answer_text = "\n\n".join(parts) if parts else None

            # файлы
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
USER_FLOW: Dict[int, str] = {}  # chat_id -> "suggest" | None

# ---------- Клавиатуры ----------
def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [["🐻 Лови краба", "❓ У меня есть вопрос"],
         ["💡 У меня есть предложение по модернизации данного бота"]],
        resize_keyboard=True
    )

def kb_categories() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[cat] for cat in CATEGORIES] + [["⬅️ Назад"]], resize_keyboard=True)

def kb_questions(category: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[it.question] for it in repo.data.get(category, [])] + [["⬅️ Назад"]], resize_keyboard=True)

# ---------- Хендлеры ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    USER_CATEGORY[update.effective_chat.id] = None
    await update.message.reply_text(
        "Привет! Я <b>PtoShkinDSU_bot</b> 🤖\nВыбирай кнопку ниже 👇",
        reply_markup=kb_main(),
        parse_mode=ParseMode.HTML
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Команды: /start /help /post. Или пользуйся кнопками ниже 👇", reply_markup=kb_main())

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

# Fuzzy-поиск по произвольному тексту
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

# /post — публикация поста в канал/группу: несколько файлов через запятую/; или без файлов
async def cmd_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Использование:
    /post <стем[,стем2,...]> | <описание>
    /post | <описание>     (без файла)
    Примеры:
      /post График ДСУ1 | 📄 График отпусков
      /post График ДСУ1, График ДСУ2 | 📎 Два графика
      /post | Объявление без вложений
    """
    if not update.message:
        return
    user_id = update.effective_user.id if update.effective_user else 0
    if not _is_post_admin(user_id):
        await update.message.reply_text("⛔ У тебя нет прав на публикацию.")
        return

    raw = (update.message.text or "").strip()
    if raw.lower().startswith("/post"):
        raw = raw.split(" ", 1)[1] if " " in raw else ""

    if "|" in raw:
        stem_part, desc_part = raw.split("|", 1)
        stems = _split_post_stems(stem_part.strip())
        desc = desc_part.strip()
    else:
        stems = []
        desc = raw.strip()

    if not desc:
        await update.message.reply_text(
            "Формат: /post <стем[,стем2,...]> | <описание>\n"
            "или:    /post | <описание>"
        )
        return

    files: List[Path] = []
    missing: List[str] = []
    for stem in stems:
        matched = _find_files_by_stem_fast(stem)
        if matched:
            files.append(matched[0])
        else:
            missing.append(stem)

    if missing:
        await update.message.reply_text("⚠️ Не найдены файлы: " + ", ".join(missing))

    try:
        await post_to_group(context, desc, files if files else None)
        await update.message.reply_text("✅ Опубликовано.")
    except Exception as e:
        logging.exception("Ошибка публикации: %s", e)
        await update.message.reply_text(f"❌ Ошибка публикации: {e}")

# Предложения
async def suggest_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    USER_FLOW[chat_id] = "suggest"
    await update.message.reply_text(
        "Напиши, пожалуйста, своё предложение одним сообщением.\n"
        "Можно приложить ссылки/описания. После отправки я всё перекину админу. ✍️",
        reply_markup=ReplyKeyboardMarkup([["⬅️ Назад"]], resize_keyboard=True)
    )

async def suggest_capture(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if USER_FLOW.get(chat_id) != "suggest":
        return
    text = (update.message.text or "").strip()
    if not text or text == "⬅️ Назад":
        USER_FLOW[chat_id] = None
        await update.message.reply_text("Отменил ввод предложения. Возвращаю в меню 👇", reply_markup=kb_main())
        return

    user = update.effective_user
    username = f"@{user.username}" if (user and user.username) else (user.full_name if user else "user")
    _append_suggestion(chat_id, user.id if user else 0, user.username if user else "", text)
    await _notify_about_suggestion(context, text, username)

    USER_FLOW[chat_id] = None
    await update.message.reply_text("Спасибо! 🚀 Твоё предложение отправлено админам. Возвращаю в меню 👇", reply_markup=kb_main())

# ---------- Роутинг ----------
def build_app() -> Application:
    # post_init выполнит _on_start после инициализации (снесёт webhook, залогирует бота, загрузит индекс файлов)
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(_on_start).build()

    # команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("post", cmd_post))
    # опционально: ручная пересборка индекса файлов
    app.add_handler(CommandHandler("reindex", cmd_reindex))

    # кнопки меню
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"^🐻 Лови краба$"), crab))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"^❓ У меня есть вопрос$"), ask_category))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"^💡 У меня есть предложение по модернизации данного бота$"), suggest_start))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"^⬅️ Назад$"), go_back))

    # категории / вопросы
    pattern = r"^(" + "|".join(map(re.escape, CATEGORIES)) + r")$"
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(pattern), choose_category))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, choose_question))

    # ввод предложений — до fuzzy
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, suggest_capture))

    # fuzzy-поиск — последним
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, fuzzy_search))

    return app

# --- Служебный хук старта: инициализация и логирование ---
async def _on_start(app: Application):
    try:
        _build_file_index()
        await app.bot.delete_webhook(drop_pending_updates=True)
        me = await app.bot.get_me()
        logger.info("Bot started as @%s (id=%s)", me.username, me.id)
    except Exception as e:
        logger.exception("Startup failed: %s", e)

# (опц.) команда для ручной пересборки индекса
async def cmd_reindex(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not u or u.id not in POST_ADMINS:
        await update.message.reply_text("⛔ Нет прав.")
        return
    _build_file_index()
    await update.message.reply_text("🔄 Индекс файлов пересобран.")

# ---------- Точка входа ----------
if __name__ == "__main__":
    print(f"[DEBUG] BASE_DIR: {BASE_DIR}")
    print(f"[DEBUG] XLSX_PATH: {XLSX_PATH} (exists={XLSX_PATH.exists()})")
    app = build_app()
    print("Bot is running…")
    app.run_polling(
        close_loop=False,
        drop_pending_updates=True,
        allowed_updates=["message"],  # меньше лишних апдейтов
        stop_signals=None,
    )
