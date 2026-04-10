import os
import asyncio
import logging
import time
import json
import hashlib
from dataclasses import dataclass, field

import aiohttp
from telegram import Update, BotCommand
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
    order_match_snapshot: set[str] = field(default_factory=set)
    last_check: float = 0


watched: dict[int, dict[str, WatchedWallet]] = {}
market_cache: dict[str, dict] = {}
chat_lang: dict[int, str] = {}

I18N = {
    "en": {
        "start": (
            "<b>Predict.fun Monitor Bot</b>\n\n"
            "/watch <code>0xAddr</code> - monitor wallet (EOA)\n"
            "/unwatch <code>0xAddr</code> - stop\n"
            "/list - watched list\n"
            "/pos <code>0xAddr</code> - view positions\n"
            "/orders <code>0xAddr</code> - recent fills\n"
            "/lang <code>en|zh</code> - switch language\n"
            "/stop - stop all"
        ),
        "usage_watch": "Usage: /watch 0xAddress",
        "invalid_address": "Invalid address",
        "already_watching": "Already watching <code>{addr}</code>",
        "loading_positions": "Loading positions...",
        "watching_ok": "Watching <code>{addr}</code>\nPositions: {count}\nInterval: {interval}s",
        "usage_unwatch": "Usage: /unwatch 0xAddress",
        "removed": "Removed <code>{addr}</code>",
        "not_found": "Not found",
        "no_watched_wallets": "No watched wallets",
        "watch_list": "<b>Watch List</b>\n",
        "usage_pos": "Usage: /pos 0xAddress",
        "usage_orders": "Usage: /orders 0xAddress",
        "orders_header": "<code>{addr}</code>\nRecent fills ({count})",
        "no_orders": "No recent fills",
        "stopped": "Stopped {count} watches",
        "lang_usage": "Usage: /lang en|zh",
        "lang_set": "Language switched to English 🇺🇸",
        "fmt_no_positions": "No positions",
        "fmt_positions_header": "{count} positions | ${total:,.2f}\n\n",
        "fmt_new": "🟢 New",
        "fmt_changed": "🔄 Changed",
        "fmt_closed": "🔴 Closed",
        "fmt_more": "\n\n...and {count} more",
        "poll_header": "<b>Predict.fun</b> <code>{addr}</code>\n\n",
        "closed_item": "Closed: {key}",
        "fills_header": "<b>Order Fill</b> <code>{addr}</code>\n\n",
    },
    "zh": {
        "start": (
            "<b>Predict.fun 监控机器人</b>\n\n"
            "/watch <code>0xAddr</code> - 监控钱包（EOA）\n"
            "/unwatch <code>0xAddr</code> - 停止监控\n"
            "/list - 查看监控列表\n"
            "/pos <code>0xAddr</code> - 查看持仓\n"
            "/orders <code>0xAddr</code> - 查看最近成交\n"
            "/lang <code>en|zh</code> - 切换语言\n"
            "/stop - 清空全部监控"
        ),
        "usage_watch": "用法：/watch 0x地址",
        "invalid_address": "地址格式无效",
        "already_watching": "已在监控 <code>{addr}</code>",
        "loading_positions": "正在加载持仓...",
        "watching_ok": "开始监控 <code>{addr}</code>\n持仓数：{count}\n轮询间隔：{interval} 秒",
        "usage_unwatch": "用法：/unwatch 0x地址",
        "removed": "已移除 <code>{addr}</code>",
        "not_found": "未找到该地址",
        "no_watched_wallets": "当前没有监控的钱包",
        "watch_list": "<b>监控列表</b>\n",
        "usage_pos": "用法：/pos 0x地址",
        "usage_orders": "用法：/orders 0x地址",
        "orders_header": "<code>{addr}</code>\n最近成交（{count}）",
        "no_orders": "暂无最近成交",
        "stopped": "已停止 {count} 个监控",
        "lang_usage": "用法：/lang en|zh",
        "lang_set": "语言已切换为中文 🇨🇳",
        "fmt_no_positions": "暂无持仓",
        "fmt_positions_header": "{count} 个持仓 | ${total:,.2f}\n\n",
        "fmt_new": "🟢 新开仓",
        "fmt_changed": "🔄 持仓变化",
        "fmt_closed": "🔴 已平仓",
        "fmt_more": "\n\n...还有 {count} 条变动",
        "poll_header": "<b>Predict.fun</b> <code>{addr}</code>\n\n",
        "closed_item": "已平仓：{key}",
        "fills_header": "<b>订单成交</b> <code>{addr}</code>\n\n",
    },
}

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


async def fetch_order_matches(
    session: aiohttp.ClientSession,
    address: str,
    first: int = 20,
) -> list[dict]:
    url = f"{PREDICT_API}/v1/orders/matches"
    params = {
        "signerAddress": address,
        "first": str(first),
    }
    try:
        async with session.get(
            url,
            params=params,
            headers=_headers(),
            timeout=aiohttp.ClientTimeout(total=12),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("data", []) if data.get("success") else []
            logger.warning(f"Orders matches API {resp.status} for {address}")
            return []
    except Exception as e:
        logger.error(f"fetch_order_matches error: {e}")
        return []


def get_lang(chat_id: int) -> str:
    return chat_lang.get(chat_id, "en")


def t(chat_id: int, key: str, **kwargs) -> str:
    lang = get_lang(chat_id)
    template = I18N.get(lang, I18N["en"]).get(key, I18N["en"].get(key, key))
    return template.format(**kwargs)

# ==================== Diff ====================

def pos_key(pos: dict) -> str:
    market_obj = pos.get("market") if isinstance(pos.get("market"), dict) else {}
    outcome_obj = pos.get("outcome") if isinstance(pos.get("outcome"), dict) else {}
    market_id = pos.get("marketId") or market_obj.get("id") or ""
    outcome_index = pos.get("outcomeIndex")
    if outcome_index is None:
        outcome_index = outcome_obj.get("index", "")
    return f"{market_id}_{outcome_index}"


def pos_hash(pos: dict) -> str:
    _, _, shares, price_c = display_fields(pos, {})
    fields = {
        "shares": shares,
        "priceCents": price_c,
        "valueUsd": str(pos.get("valueUsd") or pos.get("value_usd") or ""),
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


def _safe_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _norm_price_to_cents(price):
    p = _safe_float(price)
    if p is None:
        return None
    if 0 <= p <= 1:
        p = p * 100
    return p


def _norm_amount_to_shares(amount):
    a = _safe_float(amount)
    if a is None:
        return None
    # API may return token amount as wei string.
    if abs(a) > 1e9:
        a = a / 1e18
    return a


def _fmt_num(v, digits=4):
    if v is None:
        return "?"
    s = f"{v:.{digits}f}".rstrip("0").rstrip(".")
    return s if s else "0"


def display_fields(pos: dict, market: dict | None = None) -> tuple[str, str, str, str]:
    market = market or {}
    market_obj = pos.get("market") if isinstance(pos.get("market"), dict) else {}
    outcome_obj = pos.get("outcome") if isinstance(pos.get("outcome"), dict) else {}
    market_id = pos.get("marketId") or market_obj.get("id") or market.get("id") or "?"

    title = (
        pos.get("title")
        or market_obj.get("title")
        or market_obj.get("question")
        or market.get("title")
        or market.get("question")
        or market_id
    )

    outcome = pos.get("outcomeName") or outcome_obj.get("name") or outcome_obj.get("title")
    idx = pos.get("outcomeIndex")
    if idx is None:
        idx = outcome_obj.get("index")

    if outcome in (None, "") and idx is not None:
        outcomes = (
            market_obj.get("outcomes")
            or market_obj.get("outcomeNames")
            or market.get("outcomes")
            or market.get("outcomeNames")
            or []
        )
        try:
            i = int(idx)
            if isinstance(outcomes, list) and 0 <= i < len(outcomes):
                o = outcomes[i]
                outcome = o.get("name") if isinstance(o, dict) else str(o)
        except (ValueError, TypeError):
            pass
    if outcome in (None, ""):
        outcome = str(idx if idx is not None else "?")

    shares_num = _norm_amount_to_shares(pos.get("shares") or pos.get("quantity") or pos.get("amount"))
    shares = _fmt_num(shares_num, digits=4) if shares_num is not None else "0"

    price_raw = (
        pos.get("currentPrice")
        or pos.get("avgPrice")
        or outcome_obj.get("price")
    )
    if price_raw is None and idx is not None:
        prices = (
            market_obj.get("outcomePrices")
            or market_obj.get("prices")
            or market.get("outcomePrices")
            or market.get("prices")
            or []
        )
        try:
            i = int(idx)
            if isinstance(prices, list) and 0 <= i < len(prices):
                price_raw = prices[i]
        except (ValueError, TypeError):
            pass

    price_c = _norm_price_to_cents(price_raw)
    if price_c is None:
        value_usd = _safe_float(pos.get("valueUsd") or pos.get("value_usd"))
        if value_usd is not None and shares_num and shares_num > 0:
            price_c = value_usd / shares_num * 100

    price = f"{price_c:.2f}" if price_c is not None else "?"
    return title, outcome, shares, price


def fmt_pos(pos: dict, label: str, chat_id: int, market: dict | None = None) -> str:
    title, outcome, shares, avg = display_fields(pos, market)
    title = title[:55]

    try:
        # avg is in cents, convert to USD.
        val = f"${float(shares) * float(avg) / 100:,.2f}"
    except (ValueError, TypeError):
        val = "N/A"

    emojis = {
        "added": t(chat_id, "fmt_new"),
        "changed": t(chat_id, "fmt_changed"),
        "closed": t(chat_id, "fmt_closed"),
    }
    emoji = emojis.get(label, "📊")

    return f"{emoji}\n{title}\n{outcome}\n{shares} x {avg}c = {val}"


def fmt_summary(positions: list[dict], chat_id: int) -> str:
    if not positions:
        return t(chat_id, "fmt_no_positions")

    total = 0
    lines = []

    for p in positions[:20]:
        title, outcome, shares, price = display_fields(p, p.get("_market"))
        title = title[:40]

        try:
            # price is in cents, convert to USD.
            v = float(shares) * float(price) / 100
            total += v
            lines.append(f"- {outcome} | {shares} @ {price}c = ${v:,.2f}\n  {title}")
        except (ValueError, TypeError):
            lines.append(f"- {outcome} | {shares} @ {price}c\n  {title}")

    return t(chat_id, "fmt_positions_header", count=len(positions), total=total) + "\n\n".join(lines)


def match_key(match: dict) -> str:
    tx = match.get("transactionHash", "")
    executed_at = match.get("executedAt", "")
    amount = str(match.get("amountFilled", ""))
    return f"{tx}:{executed_at}:{amount}"


def fmt_match(match: dict) -> str:
    market = match.get("market") if isinstance(match.get("market"), dict) else {}
    taker = match.get("taker") if isinstance(match.get("taker"), dict) else {}
    outcome_obj = taker.get("outcome") if isinstance(taker.get("outcome"), dict) else {}

    title = market.get("title") or market.get("question") or str(market.get("id", "?"))
    outcome = outcome_obj.get("name") or "?"
    side = taker.get("quoteType", "?")
    shares = _norm_amount_to_shares(match.get("amountFilled"))
    shares_text = _fmt_num(shares, digits=4)
    price_c = _norm_price_to_cents(match.get("priceExecuted") or taker.get("price"))

    value_text = "N/A"
    if shares is not None and price_c is not None:
        value_text = f"${shares * price_c / 100:,.2f}"

    price_text = f"{price_c:.2f}" if price_c is not None else "?"
    return f"✅ {side} {outcome} | {shares_text} @ {price_text}c = {value_text}\n{title[:60]}"

# ==================== Telegram ====================

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(t(chat_id, "start"), parse_mode="HTML")


async def cmd_watch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if not ctx.args:
        await update.message.reply_text(t(chat_id, "usage_watch"))
        return

    addr = ctx.args[0].strip()
    if not addr.startswith("0x") or len(addr) != 42:
        await update.message.reply_text(t(chat_id, "invalid_address"))
        return

    if chat_id not in watched:
        watched[chat_id] = {}

    if addr.lower() in {a.lower() for a in watched[chat_id]}:
        await update.message.reply_text(
            t(chat_id, "already_watching", addr=fmt_addr(addr)),
            parse_mode="HTML",
        )
        return

    await update.message.reply_text(t(chat_id, "loading_positions"))

    async with aiohttp.ClientSession() as session:
        positions = await fetch_positions(session, addr)
        matches = await fetch_order_matches(session, addr, first=30)

    snapshot = {pos_key(p): pos_hash(p) for p in positions}
    watched[chat_id][addr] = WatchedWallet(
        address=addr,
        chat_id=chat_id,
        position_snapshot=snapshot,
        order_match_snapshot={match_key(m) for m in matches},
        last_check=time.time(),
    )

    await update.message.reply_text(
        t(chat_id, "watching_ok", addr=fmt_addr(addr), count=len(positions), interval=POLL_INTERVAL),
        parse_mode="HTML",
    )


async def cmd_unwatch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if not ctx.args:
        await update.message.reply_text(t(chat_id, "usage_unwatch"))
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
            t(chat_id, "removed", addr=fmt_addr(addr)),
            parse_mode="HTML",
        )
    else:
        await update.message.reply_text(t(chat_id, "not_found"))


async def cmd_list(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    wallets = watched.get(update.effective_chat.id, {})

    if not wallets:
        await update.message.reply_text(t(update.effective_chat.id, "no_watched_wallets"))
        return

    lines = [t(update.effective_chat.id, "watch_list")]
    for addr, w in wallets.items():
        lines.append(f"- <code>{fmt_addr(addr)}</code> ({len(w.position_snapshot)} pos)")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_pos(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(t(update.effective_chat.id, "usage_pos"))
        return

    addr = ctx.args[0].strip()

    async with aiohttp.ClientSession() as session:
        positions = await fetch_positions(session, addr)
        market_ids = {p.get("marketId") for p in positions if p.get("marketId")}
        markets = {mid: await fetch_market(session, mid) for mid in market_ids}
        for p in positions:
            p["_market"] = markets.get(p.get("marketId"), {})

    text = f"<code>{fmt_addr(addr)}</code>\n\n{fmt_summary(positions, update.effective_chat.id)}"
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_orders(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not ctx.args:
        await update.message.reply_text(t(chat_id, "usage_orders"))
        return

    addr = ctx.args[0].strip()
    async with aiohttp.ClientSession() as session:
        matches = await fetch_order_matches(session, addr, first=10)

    if not matches:
        await update.message.reply_text(t(chat_id, "no_orders"))
        return

    lines = [t(chat_id, "orders_header", addr=fmt_addr(addr), count=len(matches)), ""]
    lines.extend(fmt_match(m) for m in matches[:8])
    await update.message.reply_text("\n\n".join(lines), parse_mode="HTML")


async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    n = len(watched.pop(chat_id, {}))
    await update.message.reply_text(t(chat_id, "stopped", count=n))


async def cmd_lang(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not ctx.args or ctx.args[0].lower() not in ("en", "zh"):
        await update.message.reply_text(t(chat_id, "lang_usage"))
        return

    lang = ctx.args[0].lower()
    chat_lang[chat_id] = lang
    await update.message.reply_text(t(chat_id, "lang_set"))

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
                        matches = await fetch_order_matches(session, w.address, first=20)
                        market_ids = {p.get("marketId") for p in positions if p.get("marketId")}
                        markets = {mid: await fetch_market(session, mid) for mid in market_ids}
                        added, changed, closed = diff_positions(w.position_snapshot, positions)
                        new_match_keys = {match_key(m) for m in matches}
                        new_fills = [m for m in matches if match_key(m) not in w.order_match_snapshot]

                        w.position_snapshot = {pos_key(p): pos_hash(p) for p in positions}
                        w.last_check = time.time()

                        if not (added or changed or closed):
                            continue

                        parts = []
                        for p in added:
                            parts.append(fmt_pos(p, "added", chat_id, markets.get(p.get("marketId"))))
                        for p in changed:
                            parts.append(fmt_pos(p, "changed", chat_id, markets.get(p.get("marketId"))))
                        for k in closed:
                            parts.append(t(chat_id, "closed_item", key=k))

                        header = t(chat_id, "poll_header", addr=fmt_addr(w.address))
                        text = header + "\n\n".join(parts[:8])

                        if len(parts) > 8:
                            text += t(chat_id, "fmt_more", count=len(parts) - 8)

                        await app.bot.send_message(
                            chat_id=chat_id,
                            text=text,
                            parse_mode="HTML",
                            disable_web_page_preview=True,
                        )

                        if new_fills:
                            fill_lines = [fmt_match(m) for m in new_fills[:5]]
                            await app.bot.send_message(
                                chat_id=chat_id,
                                text=t(chat_id, "fills_header", addr=fmt_addr(w.address)) + "\n\n".join(fill_lines),
                                parse_mode="HTML",
                                disable_web_page_preview=True,
                            )

                        w.order_match_snapshot = set(list(new_match_keys)[:100])
                    except Exception as e:
                        logger.error(f"Poll error {addr}: {e}")

                    await asyncio.sleep(1)

            await asyncio.sleep(POLL_INTERVAL)

# ==================== Main ====================

async def on_startup(app: Application):
    await app.bot.set_my_commands(
        [
            BotCommand("start", "开始 / Start"),
            BotCommand("watch", "监控地址 / Watch wallet"),
            BotCommand("unwatch", "取消监控 / Unwatch wallet"),
            BotCommand("list", "监控列表 / Watch list"),
            BotCommand("pos", "查询持仓 / View positions"),
            BotCommand("orders", "最近成交 / Recent fills"),
            BotCommand("lang", "切换语言 / Switch language"),
            BotCommand("stop", "停止全部监控 / Stop all"),
        ]
    )
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
    app.add_handler(CommandHandler("orders", cmd_orders))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("lang", cmd_lang))

    app.post_init = on_startup

    logger.info("Bot starting...")
    app.run_polling()


if __name__ == "__main__":
    main()
