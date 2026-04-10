import os
import asyncio
import logging
import time
import json
import hashlib
from dataclasses import dataclass, field

import aiohttp
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ==================== Config ====================

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
PREDICT_API_KEY = os.environ.get("PREDICT_API_KEY", "")
PREDICT_API = "https://api.predict.fun"
POLL_INTERVAL = 15

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ==================== Data ====================

@dataclass
class WatchedWallet:
    address: str
    chat_id: int
    position_snapshot: dict = field(default_factory=dict)
    last_check: float = 0


watched: dict[int, dict[str, WatchedWallet]] = {}
market_cache: dict[str, dict] = {}

# ==================== API ====================

def _headers():
    return {
        "Content-Type": "application/json",
        "x-api-key": PREDICT_API_KEY,
    }


async def fetch_positions(session: aiohttp.ClientSession, address: str) -> list[dict]:
    url = f"{PREDICT_API}/v1/positions/{address}"
    try:
        async with session.get(
            url,
            headers=_headers(),
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("data", []) if data.get("success") else []

            logger.warning(f"Positions API {resp.status} for {address}")
            return []
    except Exception as e:
        logger.error(f"fetch_positions error: {e}")
        return []


async def fetch_market(session: aiohttp.ClientSession, market_id: str) -> dict:
    if market_id in market_cache:
        return market_cache[market_id]

    url = f"{PREDICT_API}/v1/markets/{market_id}"
    try:
        async with session.get(
            url,
            headers=_headers(),
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("success"):
                    market_cache[market_id] = data.get("data", {})
                    return market_cache[market_id]
    except Exception:
        pass

    return {}

# ==================== Diff ====================

def pos_key(pos: dict) -> str:
    return f"{pos.get('marketId', '')}_{pos.get('outcomeIndex', '')}"


def pos_hash(pos: dict) -> str:
    fields = {
        "shares": str(pos.get("shares", "")),
        "avgPrice": str(pos.get("avgPrice", "")),
    }
    return hashlib.md5(json.dumps(fields, sort_keys=True).encode()).hexdigest()


def diff_positions(old: dict, new_list: list[dict]):
    new_map = {pos_key(p): pos_hash(p) for p in new_list}

    added, changed, closed = [], [], []

    for p in new_list:
        k = pos_key(p)
        if k not in old:
            added.append(p)
        elif old[k] != new_map[k]:
            changed.append(p)

    for k in old:
        if k not in new_map:
            closed.append(k)

    return added, changed, closed

# ==================== Format ====================

def fmt_addr(addr: str) -> str:
    return f"{addr[:6]}…{addr[-4:]}"


def fmt_pos(pos: dict, label: str) -> str:
    title = (pos.get("title") or pos.get("marketId", "?"))[:55]
    outcome = pos.get("outcomeName") or pos.get("outcomeIndex", "?")
    shares = pos.get("shares", "?")
    avg = pos.get("avgPrice", "?")

    try:
        val = f"${float(shares) * float(avg):,.2f}"
    except (ValueError, TypeError):
        val = "N/A"

    emojis = {
        "added": "🟢 New",
        "changed": "🔄 Changed",
        "closed": "🔴 Closed",
    }
    emoji = emojis.get(label, "📊")

    return f"{emoji}\n{title}\n{outcome}\n{shares} x {avg}c = {val}"


def fmt_summary(positions: list[dict]) -> str:
    if not positions:
        return "No positions"

    total = 0
    lines = []

    for p in positions[:20]:
        title = (p.get("title") or p.get("marketId", "?"))[:40]
        outcome = p.get("outcomeName") or str(p.get("outcomeIndex", "?"))
        shares = p.get("shares", "0")
        price = p.get("currentPrice") or p.get("avgPrice", "?")

        try:
            v = float(shares) * float(price)
            total += v
            lines.append(f"- {outcome} | {shares} @ {price}c = ${v:,.2f}\n  {title}")
        except (ValueError, TypeError):
            lines.append(f"- {outcome} | {shares} @ {price}c\n  {title}")

    return f"{len(positions)} positions | ${total:,.2f}\n\n" + "\n\n".join(lines)

# ==================== Telegram ====================

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "<b>Predict.fun Monitor Bot</b>\n\n"
        "/watch <code>0xAddr</code> - monitor wallet (EOA)\n"
        "/unwatch <code>0xAddr</code> - stop\n"
        "/list - watched list\n"
        "/pos <code>0xAddr</code> - view positions\n"
        "/stop - stop all",
        parse_mode="HTML",
    )


async def cmd_watch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if not ctx.args:
        await update.message.reply_text("Usage: /watch 0xAddress")
        return

    addr = ctx.args[0].strip()
    if not addr.startswith("0x") or len(addr) != 42:
        await update.message.reply_text("Invalid address")
        return

    if chat_id not in watched:
        watched[chat_id] = {}

    if addr.lower() in {a.lower() for a in watched[chat_id]}:
        await update.message.reply_text(
            f"Already watching <code>{fmt_addr(addr)}</code>",
            parse_mode="HTML",
        )
        return

    await update.message.reply_text("Loading positions...")

    async with aiohttp.ClientSession() as session:
        positions = await fetch_positions(session, addr)

    snapshot = {pos_key(p): pos_hash(p) for p in positions}
    watched[chat_id][addr] = WatchedWallet(
        address=addr,
        chat_id=chat_id,
        position_snapshot=snapshot,
        last_check=time.time(),
    )

    await update.message.reply_text(
        f"Watching <code>{fmt_addr(addr)}</code>\n"
        f"Positions: {len(positions)}\n"
        f"Interval: {POLL_INTERVAL}s",
        parse_mode="HTML",
    )


async def cmd_unwatch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if not ctx.args:
        await update.message.reply_text("Usage: /unwatch 0xAddress")
        return

    addr = ctx.args[0].strip()
    to_remove = None

    for a in watched.get(chat_id, {}):
        if a.lower() == addr.lower():
            to_remove = a
            break

    if to_remove:
        del watched[chat_id][to_remove]
        await update.message.reply_text(
            f"Removed <code>{fmt_addr(addr)}</code>",
            parse_mode="HTML",
        )
    else:
        await update.message.reply_text("Not found")


async def cmd_list(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    wallets = watched.get(update.effective_chat.id, {})

    if not wallets:
        await update.message.reply_text("No watched wallets")
        return

    lines = ["<b>Watch List</b>\n"]
    for addr, w in wallets.items():
        lines.append(f"- <code>{fmt_addr(addr)}</code> ({len(w.position_snapshot)} pos)")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_pos(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: /pos 0xAddress")
        return

    addr = ctx.args[0].strip()

    async with aiohttp.ClientSession() as session:
        positions = await fetch_positions(session, addr)

    text = f"<code>{fmt_addr(addr)}</code>\n\n{fmt_summary(positions)}"
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    n = len(watched.pop(update.effective_chat.id, {}))
    await update.message.reply_text(f"Stopped {n} watches")

# ==================== Poll Loop ====================

async def poll_loop(app: Application):
    await asyncio.sleep(3)
    logger.info("Poll loop started")

    async with aiohttp.ClientSession() as session:
        while True:
            for chat_id, wallets in list(watched.items()):
                for addr, w in list(wallets.items()):
                    try:
                        positions = await fetch_positions(session, w.address)
                        added, changed, closed = diff_positions(w.position_snapshot, positions)

                        w.position_snapshot = {pos_key(p): pos_hash(p) for p in positions}
                        w.last_check = time.time()

                        if not (added or changed or closed):
                            continue

                        parts = []
                        for p in added:
                            parts.append(fmt_pos(p, "added"))
                        for p in changed:
                            parts.append(fmt_pos(p, "changed"))
                        for k in closed:
                            parts.append(f"Closed: {k}")

                        header = f"<b>Predict.fun</b> <code>{fmt_addr(w.address)}</code>\n\n"
                        text = header + "\n\n".join(parts[:8])

                        if len(parts) > 8:
                            text += f"\n\n...and {len(parts) - 8} more"

                        await app.bot.send_message(
                            chat_id=chat_id,
                            text=text,
                            parse_mode="HTML",
                            disable_web_page_preview=True,
                        )
                    except Exception as e:
                        logger.error(f"Poll error {addr}: {e}")

                    await asyncio.sleep(1)

            await asyncio.sleep(POLL_INTERVAL)

# ==================== Main ====================

async def on_startup(app: Application):
    asyncio.create_task(poll_loop(app))


def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
    if not PREDICT_API_KEY:
        raise RuntimeError("Missing PREDICT_API_KEY")

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("watch", cmd_watch))
    app.add_handler(CommandHandler("unwatch", cmd_unwatch))
    app.add_handler(CommandHandler("list", cmd_list))
    app.add_handler(CommandHandler("pos", cmd_pos))
    app.add_handler(CommandHandler("positions", cmd_pos))
    app.add_handler(CommandHandler("stop", cmd_stop))

    app.post_init = on_startup

    logger.info("Bot starting...")
    app.run_polling()


if __name__ == "__main__":
    main()
