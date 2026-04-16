import asyncio
import logging
import os
import re
from typing import Optional

from aiohttp import web
from pymongo import MongoClient
from pymongo.collection import Collection
from rapidfuzz import fuzz
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
MONGO_URI = os.getenv("MONGO_URI", "").strip()
BOT_USERNAME = os.getenv("BOT_USERNAME", "").strip().lstrip("@")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
PORT = int(os.getenv("PORT", "8080"))
DELETE_AFTER_SECONDS = int(os.getenv("DELETE_AFTER_SECONDS", "300"))


collection: Optional[Collection] = None
movie_cache: list[dict[str, str | int]] = []


def get_collection() -> Optional[Collection]:
    if not MONGO_URI:
        logger.warning("MONGO_URI is not configured")
        return None

    try:
        logger.info("Connecting to MongoDB")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client["movies_db"]
        movies = db["movies"]
        movies.create_index("msg_id", unique=True)
        movies.create_index("name")
        logger.info("MongoDB connected")
        return movies
    except Exception:
        logger.exception("MongoDB connection failed")
        return None


def load_movie_cache() -> None:
    global movie_cache

    if collection is None:
        movie_cache = []
        return

    movie_cache = list(collection.find({}, {"name": 1, "msg_id": 1, "_id": 0}))
    logger.info("Loaded %s movies into memory cache", len(movie_cache))


def clean_name(raw: str) -> str:
    text = raw.lower()
    text = re.sub(r"\.(mkv|mp4|avi)$", "", text)
    text = re.sub(r"[._]+", " ", text)
    text = re.sub(
        r"\b(1080p|720p|480p|hdrip|bluray|x264|x265|webrip|web-dl|tamil|dubbed)\b",
        "",
        text,
    )

    year_match = re.search(r"\b((?:19|20)\d{2})\b", text)
    year = year_match.group(1) if year_match else ""

    size_match = re.search(r"\b(\d+(?:\.\d+)?\s?(?:gb|mb))\b", text)
    size = size_match.group(1) if size_match else ""

    name = " ".join(text.split())
    if name.startswith("thm "):
        name = name[4:]
    elif name == "thm":
        name = ""

    final = f"THM {name.title()}".strip()
    if year:
        final += f" ({year})"
    if size:
        final += f" [{size.upper()}]"
    return final


def score_movie(query: str, movie_name: str) -> float:
    if query in movie_name:
        return 100.0

    partial = fuzz.partial_ratio(query, movie_name)
    token = fuzz.token_set_ratio(query, movie_name)
    ratio = fuzz.ratio(query, movie_name)
    return max(partial, token, ratio)


async def save_movie(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    try:
        if collection is None:
            return

        msg = update.effective_message
        if not msg or not (msg.video or msg.document):
            return

        raw = msg.caption or (msg.document.file_name if msg.document else "movie")
        name = clean_name(raw)

        collection.update_one(
            {"msg_id": msg.message_id},
            {"$setOnInsert": {"name": name.lower(), "msg_id": msg.message_id}},
            upsert=True,
        )
        if not any(item["msg_id"] == msg.message_id for item in movie_cache):
            movie_cache.append({"name": name.lower(), "msg_id": msg.message_id})
        logger.info("Saved movie: %s", name)
    except Exception:
        logger.exception("Save error")


async def search_movie(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    try:
        if collection is None:
            await update.message.reply_text("⚠️ Database not connected.")
            return

        if not update.message or not update.message.text:
            return

        query = update.message.text.strip().lower()
        if not query:
            return

        results = []
        for item in movie_cache:
            score = score_movie(query, str(item["name"]))
            if score >= 55:
                results.append((score, item))

        if not results:
            await update.message.reply_text("❌ No movie found.\nTry a different spelling.")
            return

        results = sorted(results, key=lambda item: item[0], reverse=True)[:6]

        text = f"🔎 Query: {query}\n🎬 Results: {len(results)}\n\nTap below 👇"
        buttons = []
        for _, movie in results:
            url = f"https://t.me/{BOT_USERNAME}?start={movie['msg_id']}"
            buttons.append([InlineKeyboardButton(movie["name"].title(), url=url)])

        await update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(buttons),
        )
    except Exception:
        logger.exception("Search error")
        if update.message:
            await update.message.reply_text("⚠️ Something went wrong.")


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        if not context.args:
            await update.message.reply_text("🎬 Send a movie name to search.")
            return

        if not CHANNEL_ID:
            await update.message.reply_text("⚠️ CHANNEL_ID is not configured.")
            return

        msg_id = int(context.args[0])
        sent = await context.bot.copy_message(
            chat_id=update.effective_chat.id,
            from_chat_id=CHANNEL_ID,
            message_id=msg_id,
        )

        warn = await update.message.reply_text(
            "⚠️ This file will be deleted after 5 minutes.\nForward it to Saved Messages."
        )

        await asyncio.sleep(DELETE_AFTER_SECONDS)

        for message_id in (sent.message_id, warn.message_id):
            try:
                await context.bot.delete_message(update.effective_chat.id, message_id)
            except Exception:
                logger.warning("Could not delete message %s", message_id)
    except Exception:
        logger.exception("Start command error")
        if update.message:
            await update.message.reply_text("❌ Could not send the file.")


async def healthcheck(_request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def start_health_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", healthcheck)
    app.router.add_get("/health", healthcheck)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()
    logger.info("Health server listening on port %s", PORT)
    return runner


def validate_env() -> None:
    missing = [name for name, value in {"BOT_TOKEN": BOT_TOKEN, "BOT_USERNAME": BOT_USERNAME}.items() if not value]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")


async def run_bot() -> None:
    validate_env()

    telegram_app: Application = ApplicationBuilder().token(BOT_TOKEN).build()
    telegram_app.add_handler(
        MessageHandler(
            (filters.ChatType.CHANNEL | filters.ChatType.GROUPS)
            & (filters.VIDEO | filters.Document.ALL),
            save_movie,
        )
    )
    telegram_app.add_handler(CommandHandler("start", start_cmd))
    telegram_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_movie))

    await telegram_app.initialize()
    await telegram_app.start()
    await telegram_app.updater.start_polling(drop_pending_updates=True)
    logger.info("Telegram bot started")

    try:
        await asyncio.Event().wait()
    finally:
        await telegram_app.updater.stop()
        await telegram_app.stop()
        await telegram_app.shutdown()


async def main() -> None:
    global collection
    collection = get_collection()
    load_movie_cache()
    health_runner = await start_health_server()

    try:
        await run_bot()
    finally:
        await health_runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
