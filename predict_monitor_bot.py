import os
import asyncio
import logging
import time
import json
import sqlite3
import threading
from io import BytesIO
from pathlib import Path
from dataclasses import dataclass, field

import aiohttp
from telegram import (
    Update,
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
)
from telegram.error import BadRequest
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# ==================== Config ====================

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
PREDICT_API_KEY = os.environ.get("PREDICT_API_KEY", "")
PREDICT_API = "https://api.predict.fun"
POLL_INTERVAL = 15
TX_EXPLORER_BASE = os.environ.get("TX_EXPLORER_BASE", "https://bscscan.com/tx/")
PREDICT_WEB_BASE = os.environ.get("PREDICT_WEB_BASE", "https://predict.fun/market/")
GUIDE_IMAGE_PATH = os.environ.get("GUIDE_IMAGE_PATH", os.path.join(os.path.dirname(__file__), "images", "guide_deposit_address.png"))

# How many consecutive polling failures before we alert the chat. We alert once
# per error burst — the counter resets when a request succeeds.
ERROR_ALERT_THRESHOLD = 5
LIST_PAGE_SIZE = 8


def resolve_sqlite_path() -> str:
    explicit = os.environ.get("SQLITE_PATH", "").strip()
    if explicit:
        # Support values like "$RAILWAY_VOLUME_MOUNT_PATH/bot_state.db".
        return os.path.expandvars(explicit)

    for env_key in ("RAILWAY_VOLUME_MOUNT_PATH", "RAILWAY_VOLUME_PATH"):
        mount = os.environ.get(env_key, "").strip()
        if mount:
            return str(Path(mount) / "bot_state.db")

    for candidate in ("/data", "/app/data", "/bot-volume", "/volume"):
        p = Path(candidate)
        if p.exists() and p.is_dir():
            return str(p / "bot_state.db")

    return "bot_state.db"


SQLITE_PATH = resolve_sqlite_path()

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
for noisy_logger in ("httpx", "httpcore", "telegram", "telegram.ext"):
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)

# ==================== Data ====================

@dataclass
class WatchedWallet:
    address: str
    chat_id: int
    note: str = ""
    position_snapshot: dict = field(default_factory=dict)
    order_match_snapshot: set[str] = field(default_factory=set)
    last_check: float = 0
    # Whether this wallet is muted (polled, but notifications suppressed).
    muted: bool = False
    # Cache of market titles keyed by pos_key so "closed" notifications can
    # show the human-readable title instead of a market_id key.
    position_titles: dict = field(default_factory=dict)
    # Transient runtime counters (not persisted).
    fetch_errors: int = 0
    error_notified: bool = False


watched: dict[int, dict[str, WatchedWallet]] = {}
market_cache: dict[str, dict] = {}
chat_lang: dict[int, str] = {}
db_lock = threading.Lock()

I18N = {
    "en": {
        "start": (
            "<b>Predict.fun Monitor Bot</b>\n\n"
            "/watch <code>0xAddr ...</code> - monitor one or more wallets\n"
            "/unwatch <code>addr|alias</code> - stop\n"
            "/list - watched list\n"
            "/pos <code>addr|alias</code> - view positions\n"
            "/orders <code>addr|alias</code> - recent fills\n"
            "/note <code>addr|alias remark</code> - set alias\n"
            "/mute <code>addr|alias</code> - silence alerts\n"
            "/unmute <code>addr|alias</code> - resume alerts\n"
            "/settings - preferences\n"
            "/export - export watch list\n"
            "/stop - stop all"
        ),
        "choose_lang": "🌐 Choose language:",
        "btn_watch_guide": "📖 How to find wallet address",
        "watch_guide_caption": (
            "📖 <b>How to find your Predict.fun wallet address</b>\n\n"
            "1. Go to Predict.fun and log in\n"
            "2. Click <b>Deposit</b> in the top right\n"
            "3. Find your <b>Predict Smart Wallet</b> address\n"
            "4. Click the <b>Copy</b> button to copy the address\n"
            "5. Use /watch <code>0xYourAddress</code> to start monitoring"
        ),
        "watch_guide_text": (
            "📖 <b>How to find your Predict.fun wallet address</b>\n\n"
            "1. Go to Predict.fun and log in\n"
            "2. Click <b>Deposit</b> in the top right\n"
            "3. Find your <b>Predict Smart Wallet</b> address\n"
            "4. Click the <b>Copy</b> button to copy the address\n"
            "5. Use /watch <code>0xYourAddress</code> to start monitoring"
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
        "usage_note": "Usage: /note 0xAddress your remark",
        "note_saved": "Saved note for <code>{addr}</code>: {note}",
        "note_not_found": "Address is not in watch list",
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
        "label_size": "Size",
        "label_value": "Value",
        "label_tx": "Tx",
        "shares_unit": "Shares",
        "side_bid": "Buy",
        "side_ask": "Sell",
        "view_tx": "View Tx",
        "label_order_hash": "OrderHash",
        "btn_view_positions": "📊 Pos",
        "btn_view_orders": "📜 Fills",
        "btn_unwatch": "🛑",
        "btn_mute": "🔕",
        "btn_unmute": "🔔",
        "btn_refresh": "🔄 Refresh",
        "btn_market_link": "🌐 Market",
        "label_fill": "Fill",
        "label_total_pos": "Total position",
        "label_prev_shares": "Prev",
        "label_last_check": "Last check",
        "label_muted_flag": "🔕 muted",
        "label_watching_flag": "👁 watching",
        "watching_multi": "Added {added} new, skipped {skipped} already-watching / invalid.",
        "usage_mute": "Usage: /mute addr|alias",
        "usage_unmute": "Usage: /unmute addr|alias",
        "muted_ok": "🔕 Muted <code>{addr}</code>",
        "unmuted_ok": "🔔 Unmuted <code>{addr}</code>",
        "settings_title": "<b>Settings</b>",
        "settings_lang": "🌐 Language: {lang}",
        "settings_interval": "⏱ Poll interval: {interval}s",
        "settings_watches": "👁 Watches: {count} ({muted} muted)",
        "export_caption": "Your watch list ({count} wallets)",
        "fetch_error_alert": "⚠️ Failed to reach Predict.fun API {count}× for <code>{addr}</code>. Will keep retrying silently.",
        "fetch_error_msg": "⚠️ Predict.fun API is unavailable right now. Try again in a moment.",
        "relt_never": "never",
        "relt_just_now": "just now",
        "relt_seconds": "{n}s ago",
        "relt_minutes": "{n}m ago",
        "relt_hours": "{n}h ago",
        "relt_days": "{n}d ago",
        "list_page_indicator": "{page}/{total}",
        "fills_merged_suffix": " ({count} fills)",
        "close_fallback": "(market {key})",
    },
    "zh": {
        "start": (
            "<b>Predict.fun 监控机器人</b>\n\n"
            "/watch <code>0xAddr ...</code> - 监控钱包，可一次多个\n"
            "/unwatch <code>地址或备注</code> - 停止监控\n"
            "/list - 查看监控列表\n"
            "/pos <code>地址或备注</code> - 查看持仓\n"
            "/orders <code>地址或备注</code> - 查看最近成交\n"
            "/note <code>地址或备注 新备注</code> - 设置备注\n"
            "/mute <code>地址或备注</code> - 暂停提醒\n"
            "/unmute <code>地址或备注</code> - 恢复提醒\n"
            "/settings - 偏好设置\n"
            "/export - 导出监控列表\n"
            "/stop - 清空全部监控"
        ),
        "choose_lang": "🌐 选择语言：",
        "btn_watch_guide": "📖 如何找到钱包地址",
        "watch_guide_caption": (
            "📖 <b>如何找到你的 Predict.fun 钱包地址</b>\n\n"
            "1. 打开 Predict.fun 并登录\n"
            "2. 点击右上角的 <b>Deposit</b>（存款）\n"
            "3. 找到你的 <b>Predict Smart Wallet</b> 地址\n"
            "4. 点击 <b>Copy</b>（复制）按钮复制地址\n"
            "5. 使用 /watch <code>0x你的地址</code> 开始监控"
        ),
        "watch_guide_text": (
            "📖 <b>如何找到你的 Predict.fun 钱包地址</b>\n\n"
            "1. 打开 Predict.fun 并登录\n"
            "2. 点击右上角的 <b>Deposit</b>（存款）\n"
            "3. 找到你的 <b>Predict Smart Wallet</b> 地址\n"
            "4. 点击 <b>Copy</b>（复制）按钮复制地址\n"
            "5. 使用 /watch <code>0x你的地址</code> 开始监控"
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
        "usage_note": "用法：/note 0x地址 备注",
        "note_saved": "已保存 <code>{addr}</code> 的备注：{note}",
        "note_not_found": "该地址不在监控列表中",
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
        "label_size": "数量",
        "label_value": "价值",
        "label_tx": "哈希",
        "shares_unit": "份额",
        "side_bid": "买入",
        "side_ask": "卖出",
        "view_tx": "查看交易",
        "label_order_hash": "订单哈希",
        "btn_view_positions": "📊 持仓",
        "btn_view_orders": "📜 成交",
        "btn_unwatch": "🛑",
        "btn_mute": "🔕",
        "btn_unmute": "🔔",
        "btn_refresh": "🔄 刷新",
        "btn_market_link": "🌐 查看市场",
        "label_fill": "本次成交",
        "label_total_pos": "总持仓",
        "label_prev_shares": "原持仓",
        "label_last_check": "最后检查",
        "label_muted_flag": "🔕 已静音",
        "label_watching_flag": "👁 监控中",
        "watching_multi": "新增 {added} 个监控，跳过 {skipped} 个（已存在或格式错误）。",
        "usage_mute": "用法：/mute 地址或备注",
        "usage_unmute": "用法：/unmute 地址或备注",
        "muted_ok": "🔕 已静音 <code>{addr}</code>",
        "unmuted_ok": "🔔 已恢复提醒 <code>{addr}</code>",
        "settings_title": "<b>偏好设置</b>",
        "settings_lang": "🌐 语言：{lang}",
        "settings_interval": "⏱ 轮询间隔：{interval} 秒",
        "settings_watches": "👁 监控数：{count}（已静音 {muted}）",
        "export_caption": "监控列表（{count} 个钱包）",
        "fetch_error_alert": "⚠️ 连续 {count} 次无法访问 Predict.fun API：<code>{addr}</code>。会继续静默重试。",
        "fetch_error_msg": "⚠️ Predict.fun API 暂时无响应，请稍后再试。",
        "relt_never": "未执行",
        "relt_just_now": "刚刚",
        "relt_seconds": "{n} 秒前",
        "relt_minutes": "{n} 分钟前",
        "relt_hours": "{n} 小时前",
        "relt_days": "{n} 天前",
        "list_page_indicator": "{page}/{total}",
        "fills_merged_suffix": "（共 {count} 笔）",
        "close_fallback": "（市场 {key}）",
    },
}

# ==================== API ====================


def db_conn():
    db_path = Path(SQLITE_PATH)
    if db_path.parent and str(db_path.parent) not in ("", "."):
        db_path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(SQLITE_PATH)


def init_db():
    with db_lock, db_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS watches (
                chat_id INTEGER NOT NULL,
                address TEXT NOT NULL,
                note TEXT NOT NULL DEFAULT '',
                position_snapshot TEXT NOT NULL DEFAULT '{}',
                order_match_snapshot TEXT NOT NULL DEFAULT '[]',
                last_check REAL NOT NULL DEFAULT 0,
                muted INTEGER NOT NULL DEFAULT 0,
                position_titles TEXT NOT NULL DEFAULT '{}',
                PRIMARY KEY (chat_id, address)
            )
            """
        )
        # Tolerate upgrades from older schemas that pre-date these columns.
        for column, ddl in (
            ("muted", "ALTER TABLE watches ADD COLUMN muted INTEGER NOT NULL DEFAULT 0"),
            ("position_titles", "ALTER TABLE watches ADD COLUMN position_titles TEXT NOT NULL DEFAULT '{}'"),
        ):
            try:
                conn.execute(ddl)
            except sqlite3.OperationalError:
                # Column already exists — ignore.
                pass
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_lang (
                chat_id INTEGER PRIMARY KEY,
                lang TEXT NOT NULL
            )
            """
        )
        conn.commit()


def save_watch(w: WatchedWallet):
    with db_lock, db_conn() as conn:
        conn.execute(
            """
            INSERT INTO watches(chat_id, address, note, position_snapshot, order_match_snapshot, last_check, muted, position_titles)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(chat_id, address) DO UPDATE SET
              note=excluded.note,
              position_snapshot=excluded.position_snapshot,
              order_match_snapshot=excluded.order_match_snapshot,
              last_check=excluded.last_check,
              muted=excluded.muted,
              position_titles=excluded.position_titles
            """,
            (
                w.chat_id,
                w.address,
                w.note,
                json.dumps(w.position_snapshot, ensure_ascii=False),
                json.dumps(sorted(w.order_match_snapshot), ensure_ascii=False),
                w.last_check,
                1 if w.muted else 0,
                json.dumps(w.position_titles, ensure_ascii=False),
            ),
        )
        conn.commit()


def delete_watch(chat_id: int, address: str):
    with db_lock, db_conn() as conn:
        conn.execute("DELETE FROM watches WHERE chat_id=? AND lower(address)=lower(?)", (chat_id, address))
        conn.commit()


def delete_all_watches(chat_id: int):
    with db_lock, db_conn() as conn:
        conn.execute("DELETE FROM watches WHERE chat_id=?", (chat_id,))
        conn.commit()


def save_chat_lang(chat_id: int, lang: str):
    with db_lock, db_conn() as conn:
        conn.execute(
            """
            INSERT INTO chat_lang(chat_id, lang) VALUES(?,?)
            ON CONFLICT(chat_id) DO UPDATE SET lang=excluded.lang
            """,
            (chat_id, lang),
        )
        conn.commit()


def load_state():
    with db_lock, db_conn() as conn:
        for chat_id, lang in conn.execute("SELECT chat_id, lang FROM chat_lang"):
            chat_lang[int(chat_id)] = lang

        for row in conn.execute(
            """
            SELECT chat_id, address, note, position_snapshot, order_match_snapshot,
                   last_check, muted, position_titles
            FROM watches
            """
        ):
            (
                chat_id,
                address,
                note,
                pos_json,
                matches_json,
                last_check,
                muted,
                titles_json,
            ) = row
            if int(chat_id) not in watched:
                watched[int(chat_id)] = {}
            watched[int(chat_id)][address] = WatchedWallet(
                address=address,
                chat_id=int(chat_id),
                note=note or "",
                position_snapshot=json.loads(pos_json or "{}"),
                order_match_snapshot=set(json.loads(matches_json or "[]")),
                last_check=float(last_check or 0),
                muted=bool(muted),
                position_titles=json.loads(titles_json or "{}"),
            )


def db_counts() -> tuple[int, int]:
    with db_lock, db_conn() as conn:
        chats = conn.execute("SELECT COUNT(*) FROM chat_lang").fetchone()[0]
        watches_count = conn.execute("SELECT COUNT(*) FROM watches").fetchone()[0]
    return int(chats), int(watches_count)

def _headers():
    return {
        "Content-Type": "application/json",
        "x-api-key": PREDICT_API_KEY,
    }


async def fetch_positions(session: aiohttp.ClientSession, address: str) -> list[dict] | None:
    """Return positions list, or ``None`` on transport/HTTP error.

    An empty list means the wallet genuinely has no positions; ``None`` lets
    callers track consecutive failures for alerting.
    """
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
            return None
    except Exception as e:
        logger.error(f"fetch_positions error: {e}")
        return None


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
) -> list[dict] | None:
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
            return None
    except Exception as e:
        logger.error(f"fetch_order_matches error: {e}")
        return None


def get_lang(chat_id: int) -> str:
    return chat_lang.get(chat_id, "en")


def t(chat_id: int, key: str, **kwargs) -> str:
    lang = get_lang(chat_id)
    template = I18N.get(lang, I18N["en"]).get(key, I18N["en"].get(key, key))
    return template.format(**kwargs)

# ==================== Helpers ====================


def resolve_addr(chat_id: int, arg: str) -> str | None:
    """Map a user-supplied argument to a canonical watched address.

    Accepts either the raw 0x address or a previously saved note/alias
    (case-insensitive match on either). Returns the canonical stored
    address, or ``None`` if nothing matched and the arg isn't a well-formed
    0x address.
    """
    if not arg:
        return None
    arg_lower = arg.lower().strip()
    wallets = watched.get(chat_id, {})
    for a, w in wallets.items():
        if a.lower() == arg_lower:
            return a
        if w.note and w.note.lower() == arg_lower:
            return a
    if arg.startswith("0x") and len(arg) == 42:
        return arg
    return None


def market_url(market: dict | None) -> str | None:
    if not isinstance(market, dict):
        return None
    slug = market.get("slug") or market.get("marketSlug") or market.get("id")
    if not slug:
        return None
    return f"{PREDICT_WEB_BASE.rstrip('/')}/{slug}"


def _relative_time(chat_id: int, ts: float) -> str:
    if not ts:
        return t(chat_id, "relt_never")
    delta = max(0, time.time() - ts)
    if delta < 10:
        return t(chat_id, "relt_just_now")
    if delta < 60:
        return t(chat_id, "relt_seconds", n=int(delta))
    if delta < 3600:
        return t(chat_id, "relt_minutes", n=int(delta / 60))
    if delta < 86400:
        return t(chat_id, "relt_hours", n=int(delta / 3600))
    return t(chat_id, "relt_days", n=int(delta / 86400))


def consolidate_fills(matches: list[dict]) -> list[dict]:
    """Merge partial fills of the same order into a single synthetic entry.

    Predict.fun can report a single user order as several separate matches
    (taker side partial fills). Displaying each as its own notification is
    noisy, so we collapse them into one entry summed across amounts.
    """
    groups: dict[str, list[dict]] = {}
    order: list[str] = []
    for m in matches:
        taker = m.get("taker") if isinstance(m.get("taker"), dict) else {}
        key = (
            m.get("orderHash")
            or taker.get("orderHash")
            or m.get("transactionHash")
            or match_key(m)
        )
        key = str(key)
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(m)

    result = []
    for key in order:
        parts = groups[key]
        if len(parts) == 1:
            result.append(parts[0])
            continue
        merged = dict(parts[0])
        shares_sum = 0.0
        taker_sum = 0.0
        maker_sum = 0.0
        for p in parts:
            s = _norm_amount_to_shares(p.get("amountFilled"))
            t_ = _norm_amount_to_shares(p.get("takerAmountFilled"))
            ma = _norm_amount_to_shares(p.get("makerAmountFilled"))
            if s is not None:
                shares_sum += s
            if t_ is not None:
                taker_sum += t_
            if ma is not None:
                maker_sum += ma
        # Store merged amounts as pre-scaled floats so _norm_amount_to_shares
        # leaves them unchanged downstream.
        merged["amountFilled"] = shares_sum
        merged["takerAmountFilled"] = taker_sum
        merged["makerAmountFilled"] = maker_sum
        merged["_merged_count"] = len(parts)
        result.append(merged)
    return result


# ==================== Diff ====================

def pos_key(pos: dict) -> str:
    market_obj = pos.get("market") if isinstance(pos.get("market"), dict) else {}
    outcome_obj = pos.get("outcome") if isinstance(pos.get("outcome"), dict) else {}
    market_id = pos.get("marketId") or market_obj.get("id") or ""
    outcome_index = pos.get("outcomeIndex")
    if outcome_index is None:
        outcome_index = outcome_obj.get("index", "")
    return f"{market_id}_{outcome_index}"


def pos_size(pos: dict) -> float | None:
    """Return current share count for a position, normalized to a float."""
    return _norm_amount_to_shares(pos.get("shares") or pos.get("quantity") or pos.get("amount"))


def _coerce_snapshot_value(v):
    """Legacy snapshots stored md5 hashes (strings); new ones store share counts (floats).

    Returns a float for new-style entries, or None for legacy/unknown entries so
    the first poll after an upgrade silently migrates without spamming the chat.
    """
    if isinstance(v, (int, float)):
        return float(v)
    return None


def diff_positions(old: dict, new_list: list[dict]):
    """Compute added/changed/closed positions.

    For ``changed`` entries we return ``(position, previous_shares)`` so the
    notification can display the delta (e.g. "5 → 12"). ``previous_shares`` is
    ``None`` when migrating from the legacy hash-based snapshot.
    """
    added: list[dict] = []
    changed: list[tuple[dict, float | None]] = []
    closed: list[str] = []

    new_map = {pos_key(p): pos_size(p) for p in new_list}

    for p in new_list:
        k = pos_key(p)
        if k not in old:
            added.append(p)
            continue
        old_val = _coerce_snapshot_value(old[k])
        new_val = new_map[k]
        if old_val is None:
            # Legacy snapshot — refresh silently without a notification.
            continue
        if new_val is None:
            continue
        if abs(new_val - old_val) > 1e-9:
            changed.append((p, old_val))

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
    # Some endpoints return fixed-point numbers where cents are scaled by 1e17.
    if abs(p) >= 1e6:
        p = p / 1e17
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

    # Prefer the user's entry/average cost basis over the current mark price.
    # Position-change notifications are about what the watched wallet did, so
    # "价值" should reflect what they actually spent on those shares rather
    # than today's market quote (which may have already drifted).
    price_raw = (
        pos.get("avgPrice")
        or pos.get("averagePrice")
        or pos.get("entryPrice")
        or pos.get("avgEntryPrice")
        or pos.get("costBasis")
        or pos.get("currentPrice")
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
        value_usd = _safe_float(
            pos.get("costUsd")
            or pos.get("cost_usd")
            or pos.get("valueUsd")
            or pos.get("value_usd")
        )
        if value_usd is not None and shares_num and shares_num > 0:
            price_c = value_usd / shares_num * 100

    price = f"{price_c:.2f}" if price_c is not None else "?"
    return title, outcome, shares, price


def _html_escape(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _title_html(title: str, market: dict | None) -> str:
    """Render a market title, turning it into a hyperlink when possible."""
    safe_title = _html_escape(title or "?")
    url = market_url(market)
    if url:
        return f'<a href="{url}">{safe_title}</a>'
    return safe_title


def fmt_pos(
    pos: dict,
    label: str,
    chat_id: int,
    market: dict | None = None,
    prev_shares: float | None = None,
) -> str:
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

    # When the size changed, surface the delta so the user can see whether the
    # wallet added to or trimmed an existing position.
    size_line = f"💹 {t(chat_id, 'label_size')}: <code>{shares}</code> {t(chat_id, 'shares_unit')} @ <code>{avg}c</code>"
    if label == "changed" and prev_shares is not None:
        try:
            new_num = float(shares)
        except (ValueError, TypeError):
            new_num = None
        if new_num is not None:
            delta = new_num - prev_shares
            arrow = "▲" if delta > 0 else ("▼" if delta < 0 else "=")
            prev_text = _fmt_num(prev_shares, digits=4)
            delta_text = _fmt_num(abs(delta), digits=4)
            size_line = (
                f"💹 {t(chat_id, 'label_size')}: <code>{prev_text}</code> → "
                f"<code>{shares}</code> {t(chat_id, 'shares_unit')} "
                f"({arrow}{delta_text}) @ <code>{avg}c</code>"
            )

    combined_market = market or pos.get("_market")
    return (
        f"{emoji} <b>{_title_html(title, combined_market)}</b>\n"
        f"✅ <b>{outcome}</b>\n"
        f"{size_line}\n"
        f"💰 {t(chat_id, 'label_value')}: <b>{val}</b>"
    )


def fmt_summary(positions: list[dict], chat_id: int) -> str:
    if not positions:
        return t(chat_id, "fmt_no_positions")

    total = 0
    lines = []

    for p in positions[:20]:
        title, outcome, shares, price = display_fields(p, p.get("_market"))
        title = title[:40]
        title_html = _title_html(title, p.get("_market"))

        try:
            # price is in cents, convert to USD.
            v = float(shares) * float(price) / 100
            total += v
            lines.append(f"• {outcome} | {shares} @ {price}c = ${v:,.2f}\n  {title_html}")
        except (ValueError, TypeError):
            lines.append(f"• {outcome} | {shares} @ {price}c\n  {title_html}")

    return t(chat_id, "fmt_positions_header", count=len(positions), total=total) + "\n\n".join(lines)


def match_key(match: dict) -> str:
    tx = match.get("transactionHash", "")
    executed_at = match.get("executedAt", "")
    amount = str(match.get("amountFilled", ""))
    return f"{tx}:{executed_at}:{amount}"


def _side_text(side: str, chat_id: int) -> str:
    s = (side or "").lower()
    if s == "bid":
        return t(chat_id, "side_bid")
    if s == "ask":
        return t(chat_id, "side_ask")
    return side or "?"


def _position_lookup_key(market_id, outcome_index) -> str:
    return f"{market_id or ''}_{outcome_index if outcome_index is not None else ''}"


def fmt_match(
    match: dict,
    chat_id: int,
    positions_by_key: dict | None = None,
) -> str:
    market = match.get("market") if isinstance(match.get("market"), dict) else {}
    taker = match.get("taker") if isinstance(match.get("taker"), dict) else {}
    outcome_obj = taker.get("outcome") if isinstance(taker.get("outcome"), dict) else {}

    title = market.get("title") or market.get("question") or str(market.get("id", "?"))
    outcome = outcome_obj.get("name") or "?"
    side = _side_text(taker.get("quoteType", "?"), chat_id)
    shares = _norm_amount_to_shares(match.get("amountFilled"))
    shares_text = _fmt_num(shares, digits=4)

    # Derive executed price and total USD value from on-chain filled amounts
    # whenever possible — these are the ground truth from the settlement. The
    # API's `priceExecuted` / `taker.price` fields are only used as a fallback,
    # because they can arrive in varying scales that are easy to misinterpret.
    maker_amt = _norm_amount_to_shares(match.get("makerAmountFilled"))
    taker_amt = _norm_amount_to_shares(match.get("takerAmountFilled"))

    price_c = None
    total_usd = None
    if (
        shares is not None and shares > 0
        and maker_amt is not None and maker_amt > 0
        and taker_amt is not None and taker_amt > 0
    ):
        # One side is the position token (matches `amountFilled`), the other is
        # the collateral (USD). Pick whichever is closer to `shares` as the
        # position side; the other is the collateral total.
        if abs(maker_amt - shares) <= abs(taker_amt - shares):
            total_usd = taker_amt
        else:
            total_usd = maker_amt
        price_c = total_usd / shares * 100

    if price_c is None:
        price_c = _norm_price_to_cents(match.get("priceExecuted") or taker.get("price"))

    if total_usd is not None:
        value_text = f"${total_usd:,.2f}"
    elif shares is not None and price_c is not None:
        value_text = f"${shares * price_c / 100:,.2f}"
    else:
        value_text = "N/A"

    price_text = f"{price_c:.2f}" if price_c is not None else "?"
    tx = match.get("transactionHash") or "N/A"
    tx_short = f"{tx[:10]}...{tx[-6:]}" if isinstance(tx, str) and len(tx) > 20 else str(tx)
    tx_line = f"{t(chat_id, 'label_tx')}: <code>{tx_short}</code>"
    if isinstance(tx, str) and tx.startswith("0x"):
        tx_url = f"{TX_EXPLORER_BASE.rstrip('/')}/{tx}"
        tx_line += f' | <a href="{tx_url}">{t(chat_id, "view_tx")}</a>'
    order_hash = match.get("orderHash") or taker.get("orderHash")
    order_line = ""
    if order_hash:
        oh = str(order_hash)
        oh_short = f"{oh[:10]}...{oh[-6:]}" if len(oh) > 20 else oh
        order_line = f"\n{t(chat_id, 'label_order_hash')}: <code>{oh_short}</code>"

    # If we have the wallet's current positions, surface the total holding for
    # this market+outcome so the user can distinguish "this fill" from "what
    # they now hold in total" — especially when adding to an existing stake.
    total_line = ""
    if positions_by_key:
        market_id = market.get("id") or ""
        outcome_idx = outcome_obj.get("index")
        lookup_key = _position_lookup_key(market_id, outcome_idx)
        current_pos = positions_by_key.get(lookup_key)
        if current_pos:
            total_shares = pos_size(current_pos)
            _, _, total_shares_text, total_price = display_fields(
                current_pos, current_pos.get("_market")
            )
            try:
                total_val = f"${float(total_shares_text) * float(total_price) / 100:,.2f}"
            except (ValueError, TypeError):
                total_val = "N/A"
            if total_shares is not None:
                total_line = (
                    f"\n📦 {t(chat_id, 'label_total_pos')}: "
                    f"<b>{total_shares_text}</b> {t(chat_id, 'shares_unit')} "
                    f"@ <code>{total_price}c</code> = <b>{total_val}</b>"
                )

    merged_suffix = ""
    merged_count = match.get("_merged_count")
    if isinstance(merged_count, int) and merged_count > 1:
        merged_suffix = t(chat_id, "fills_merged_suffix", count=merged_count)

    title_html = _title_html(title[:60], market)
    return (
        f"✅ {side} {outcome} | "
        f"{t(chat_id, 'label_fill')} {shares_text} @ {price_text}c = {value_text}{merged_suffix}\n"
        f"{title_html}{total_line}\n"
        f"{tx_line}{order_line}"
    )

# ==================== Telegram ====================


def _start_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🇺🇸 English", callback_data="lang_en"),
                InlineKeyboardButton("🇨🇳 中文", callback_data="lang_zh"),
            ],
            [InlineKeyboardButton(t(chat_id, "btn_watch_guide"), callback_data="watch_guide")],
        ]
    )


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(
        t(chat_id, "start") + "\n\n" + t(chat_id, "choose_lang"),
        parse_mode="HTML",
        reply_markup=_start_keyboard(chat_id),
    )


async def _fetch_positions_with_markets(session, addr):
    """Fetch positions + their markets, returning (positions, positions_by_key).

    ``positions`` is ``None`` when the upstream call failed (vs ``[]`` for a
    wallet that legitimately has none).
    """
    positions = await fetch_positions(session, addr)
    if positions is None:
        return None, {}
    market_ids = {p.get("marketId") for p in positions if p.get("marketId")}
    markets = {mid: await fetch_market(session, mid) for mid in market_ids}
    for p in positions:
        p["_market"] = markets.get(p.get("marketId"), {})
    return positions, {pos_key(p): p for p in positions}


async def _render_positions(chat_id: int, addr: str) -> tuple[str, InlineKeyboardMarkup]:
    async with aiohttp.ClientSession() as session:
        positions, _ = await _fetch_positions_with_markets(session, addr)
    if positions is None:
        text = t(chat_id, "fetch_error_msg")
    else:
        text = f"<code>{addr}</code>\n\n{fmt_summary(positions, chat_id)}"
    markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(t(chat_id, "btn_refresh"), callback_data=f"refresh_pos:{addr}"),
                InlineKeyboardButton(t(chat_id, "btn_view_orders"), callback_data=f"orders:{addr}"),
            ]
        ]
    )
    return text, markup


async def _render_orders(chat_id: int, addr: str) -> str:
    async with aiohttp.ClientSession() as session:
        matches = await fetch_order_matches(session, addr, first=10)
        positions, positions_by_key = await _fetch_positions_with_markets(session, addr)

    if matches is None and positions is None:
        return t(chat_id, "fetch_error_msg")
    matches = matches or []
    if not matches:
        return f"<code>{addr}</code>\n\n{t(chat_id, 'no_orders')}"
    matches = consolidate_fills(matches)
    lines = [t(chat_id, "orders_header", addr=fmt_addr(addr), count=len(matches)), ""]
    lines.extend(fmt_match(m, chat_id, positions_by_key) for m in matches[:8])
    return "\n\n".join(lines)


async def cmd_watch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if not ctx.args:
        await update.message.reply_text(t(chat_id, "usage_watch"))
        return

    # Multi-address mode: if every argument is a well-formed 0x address, watch
    # them all. Otherwise, fall back to "<addr> <note ...>" semantics.
    def _is_addr(a: str) -> bool:
        return a.startswith("0x") and len(a) == 42

    raw_args = [a.strip() for a in ctx.args if a.strip()]
    multi_mode = len(raw_args) > 1 and all(_is_addr(a) for a in raw_args)

    if multi_mode:
        addresses = raw_args
        base_note = ""
    else:
        addresses = [raw_args[0]]
        base_note = " ".join(raw_args[1:]).strip()

    watched.setdefault(chat_id, {})
    status = await update.message.reply_text(t(chat_id, "loading_positions"))

    added = 0
    skipped = 0
    last_addr = ""
    last_count = 0

    async with aiohttp.ClientSession() as session:
        for addr in addresses:
            if not _is_addr(addr):
                skipped += 1
                continue
            if addr.lower() in {a.lower() for a in watched[chat_id]}:
                skipped += 1
                continue

            positions = await fetch_positions(session, addr)
            matches = await fetch_order_matches(session, addr, first=30)
            if positions is None and matches is None:
                # Treat as skipped — can't establish a snapshot reliably.
                skipped += 1
                continue
            positions = positions or []
            matches = matches or []

            snapshot = {pos_key(p): pos_size(p) for p in positions}
            title_cache = {}
            for p in positions:
                title, _, _, _ = display_fields(p, p.get("_market"))
                title_cache[pos_key(p)] = title[:80]
            watched[chat_id][addr] = WatchedWallet(
                address=addr,
                chat_id=chat_id,
                note=base_note if len(addresses) == 1 else "",
                position_snapshot=snapshot,
                position_titles=title_cache,
                order_match_snapshot={match_key(m) for m in matches},
                last_check=time.time(),
            )
            save_watch(watched[chat_id][addr])
            added += 1
            last_addr = addr
            last_count = len(positions)

    if multi_mode:
        final_text = t(chat_id, "watching_multi", added=added, skipped=skipped)
    elif added == 1:
        final_text = t(
            chat_id,
            "watching_ok",
            addr=fmt_addr(last_addr),
            count=last_count,
            interval=POLL_INTERVAL,
        )
    elif skipped == 1 and added == 0 and _is_addr(raw_args[0]):
        final_text = t(chat_id, "already_watching", addr=fmt_addr(raw_args[0]))
    else:
        final_text = t(chat_id, "invalid_address")

    try:
        await status.edit_text(final_text, parse_mode="HTML")
    except BadRequest:
        await update.message.reply_text(final_text, parse_mode="HTML")


async def cmd_unwatch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if not ctx.args:
        await update.message.reply_text(t(chat_id, "usage_unwatch"))
        return

    addr = resolve_addr(chat_id, ctx.args[0])
    if not addr or addr not in watched.get(chat_id, {}):
        await update.message.reply_text(t(chat_id, "not_found"))
        return

    del watched[chat_id][addr]
    delete_watch(chat_id, addr)
    await update.message.reply_text(
        t(chat_id, "removed", addr=fmt_addr(addr)),
        parse_mode="HTML",
    )


def _render_list_page(chat_id: int, page: int) -> tuple[str, InlineKeyboardMarkup | None]:
    wallets = list(watched.get(chat_id, {}).items())
    total_pages = max(1, (len(wallets) + LIST_PAGE_SIZE - 1) // LIST_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * LIST_PAGE_SIZE
    chunk = wallets[start : start + LIST_PAGE_SIZE]

    lines = [t(chat_id, "watch_list")]
    keyboard: list[list[InlineKeyboardButton]] = []
    for addr, w in chunk:
        flag = t(chat_id, "label_muted_flag") if w.muted else t(chat_id, "label_watching_flag")
        note = f" · <b>{w.note}</b>" if w.note else ""
        relt = _relative_time(chat_id, w.last_check)
        lines.append(
            f"{flag} <code>{addr}</code>{note}\n"
            f"    {len(w.position_snapshot)} pos · {t(chat_id, 'label_last_check')}: {relt}"
        )
        label_title = (w.note or fmt_addr(addr))[:18]
        mute_label = t(chat_id, "btn_unmute") if w.muted else t(chat_id, "btn_mute")
        keyboard.append(
            [
                InlineKeyboardButton(
                    f"{t(chat_id, 'btn_view_positions')} {label_title}",
                    callback_data=f"pos:{addr}",
                ),
                InlineKeyboardButton(
                    t(chat_id, "btn_view_orders"), callback_data=f"orders:{addr}"
                ),
                InlineKeyboardButton(mute_label, callback_data=f"togglemute:{addr}"),
                InlineKeyboardButton(
                    t(chat_id, "btn_unwatch"), callback_data=f"unwatch:{addr}"
                ),
            ]
        )
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅", callback_data=f"list_page:{page - 1}"))
        nav.append(
            InlineKeyboardButton(
                t(chat_id, "list_page_indicator", page=page + 1, total=total_pages),
                callback_data="noop",
            )
        )
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton("➡", callback_data=f"list_page:{page + 1}"))
        keyboard.append(nav)

    markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    return "\n".join(lines), markup


async def cmd_list(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not watched.get(chat_id):
        await update.message.reply_text(t(chat_id, "no_watched_wallets"))
        return
    text, markup = _render_list_page(chat_id, 0)
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=markup)


async def cmd_pos(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not ctx.args:
        await update.message.reply_text(t(chat_id, "usage_pos"))
        return

    addr = resolve_addr(chat_id, ctx.args[0])
    if not addr:
        await update.message.reply_text(t(chat_id, "invalid_address"))
        return

    status = await update.message.reply_text(t(chat_id, "loading_positions"))
    text, markup = await _render_positions(chat_id, addr)
    try:
        await status.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=markup,
            disable_web_page_preview=True,
        )
    except BadRequest:
        await update.message.reply_text(
            text, parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True
        )


async def cmd_orders(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not ctx.args:
        await update.message.reply_text(t(chat_id, "usage_orders"))
        return

    addr = resolve_addr(chat_id, ctx.args[0])
    if not addr:
        await update.message.reply_text(t(chat_id, "invalid_address"))
        return

    status = await update.message.reply_text(t(chat_id, "loading_positions"))
    text = await _render_orders(chat_id, addr)
    try:
        await status.edit_text(text, parse_mode="HTML", disable_web_page_preview=True)
    except BadRequest:
        await update.message.reply_text(text, parse_mode="HTML", disable_web_page_preview=True)


async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    n = len(watched.pop(chat_id, {}))
    delete_all_watches(chat_id)
    await update.message.reply_text(t(chat_id, "stopped", count=n))


async def cmd_lang(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not ctx.args or ctx.args[0].lower() not in ("en", "zh"):
        await update.message.reply_text(t(chat_id, "lang_usage"))
        return

    lang = ctx.args[0].lower()
    chat_lang[chat_id] = lang
    save_chat_lang(chat_id, lang)
    await update.message.reply_text(t(chat_id, "lang_set"))


async def cmd_note(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if len(ctx.args) < 2:
        await update.message.reply_text(t(chat_id, "usage_note"))
        return

    addr = resolve_addr(chat_id, ctx.args[0])
    target = watched.get(chat_id, {}).get(addr) if addr else None
    if not target:
        await update.message.reply_text(t(chat_id, "note_not_found"))
        return

    target.note = " ".join(ctx.args[1:]).strip()
    save_watch(target)
    await update.message.reply_text(
        t(chat_id, "note_saved", addr=fmt_addr(addr), note=target.note),
        parse_mode="HTML",
    )


async def _set_mute(update: Update, ctx: ContextTypes.DEFAULT_TYPE, muted: bool):
    chat_id = update.effective_chat.id
    usage_key = "usage_mute" if muted else "usage_unmute"
    if not ctx.args:
        await update.message.reply_text(t(chat_id, usage_key))
        return
    addr = resolve_addr(chat_id, ctx.args[0])
    target = watched.get(chat_id, {}).get(addr) if addr else None
    if not target:
        await update.message.reply_text(t(chat_id, "not_found"))
        return
    target.muted = muted
    save_watch(target)
    key = "muted_ok" if muted else "unmuted_ok"
    await update.message.reply_text(
        t(chat_id, key, addr=fmt_addr(addr)), parse_mode="HTML"
    )


async def cmd_mute(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _set_mute(update, ctx, True)


async def cmd_unmute(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _set_mute(update, ctx, False)


async def cmd_settings(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    wallets = watched.get(chat_id, {})
    muted_count = sum(1 for w in wallets.values() if w.muted)
    lang_label = "🇨🇳 中文" if get_lang(chat_id) == "zh" else "🇺🇸 English"
    body = "\n".join(
        [
            t(chat_id, "settings_title"),
            "",
            t(chat_id, "settings_lang", lang=lang_label),
            t(chat_id, "settings_interval", interval=POLL_INTERVAL),
            t(chat_id, "settings_watches", count=len(wallets), muted=muted_count),
        ]
    )
    await update.message.reply_text(
        body,
        parse_mode="HTML",
        reply_markup=_start_keyboard(chat_id),
    )


async def cmd_export(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    wallets = watched.get(chat_id, {})
    payload = {
        "chat_id": chat_id,
        "lang": get_lang(chat_id),
        "exported_at": int(time.time()),
        "watches": [
            {
                "address": a,
                "note": w.note,
                "muted": w.muted,
                "positions": len(w.position_snapshot),
                "last_check": w.last_check,
            }
            for a, w in wallets.items()
        ],
    }
    buf = BytesIO(json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8"))
    buf.seek(0)
    await ctx.bot.send_document(
        chat_id=chat_id,
        document=InputFile(buf, filename="predict_watches.json"),
        caption=t(chat_id, "export_caption", count=len(wallets)),
    )


# ==================== Callback handling ====================


async def _show_positions_via_callback(query, ctx, chat_id: int, addr: str, edit: bool):
    text, markup = await _render_positions(chat_id, addr)
    if edit:
        try:
            await query.edit_message_text(
                text,
                parse_mode="HTML",
                reply_markup=markup,
                disable_web_page_preview=True,
            )
            return
        except BadRequest:
            pass
    await ctx.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode="HTML",
        reply_markup=markup,
        disable_web_page_preview=True,
    )


async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    data = query.data or ""

    if data == "noop":
        return

    if data in ("lang_en", "lang_zh"):
        lang = data.split("_")[1]
        chat_lang[chat_id] = lang
        save_chat_lang(chat_id, lang)
        try:
            await query.edit_message_text(
                t(chat_id, "start") + "\n\n" + t(chat_id, "choose_lang"),
                parse_mode="HTML",
                reply_markup=_start_keyboard(chat_id),
            )
        except BadRequest:
            pass
        return

    if data.startswith("pos:") or data.startswith("refresh_pos:"):
        addr = data.split(":", 1)[1].strip()
        if not addr.startswith("0x") or len(addr) != 42:
            await ctx.bot.send_message(chat_id=chat_id, text=t(chat_id, "invalid_address"))
            return
        await _show_positions_via_callback(
            query, ctx, chat_id, addr, edit=data.startswith("refresh_pos:")
        )
        return

    if data.startswith("orders:"):
        addr = data.split(":", 1)[1].strip()
        if not addr.startswith("0x") or len(addr) != 42:
            await ctx.bot.send_message(chat_id=chat_id, text=t(chat_id, "invalid_address"))
            return
        text = await _render_orders(chat_id, addr)
        await ctx.bot.send_message(
            chat_id=chat_id, text=text, parse_mode="HTML", disable_web_page_preview=True
        )
        return

    if data.startswith("unwatch:"):
        addr = data.split(":", 1)[1].strip()
        if addr in watched.get(chat_id, {}):
            del watched[chat_id][addr]
            delete_watch(chat_id, addr)
        # Refresh the list in place.
        if watched.get(chat_id):
            text, markup = _render_list_page(chat_id, 0)
            try:
                await query.edit_message_text(text, parse_mode="HTML", reply_markup=markup)
            except BadRequest:
                pass
        else:
            try:
                await query.edit_message_text(t(chat_id, "no_watched_wallets"))
            except BadRequest:
                pass
        return

    if data.startswith("togglemute:"):
        addr = data.split(":", 1)[1].strip()
        target = watched.get(chat_id, {}).get(addr)
        if target:
            target.muted = not target.muted
            save_watch(target)
            text, markup = _render_list_page(chat_id, 0)
            try:
                await query.edit_message_text(text, parse_mode="HTML", reply_markup=markup)
            except BadRequest:
                pass
        return

    if data.startswith("list_page:"):
        try:
            page = int(data.split(":", 1)[1])
        except ValueError:
            return
        text, markup = _render_list_page(chat_id, page)
        try:
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=markup)
        except BadRequest:
            pass
        return

    if data == "watch_guide":
        guide_path = Path(GUIDE_IMAGE_PATH)
        if guide_path.exists():
            with open(guide_path, "rb") as photo:
                await ctx.bot.send_photo(
                    chat_id=chat_id,
                    photo=photo,
                    caption=t(chat_id, "watch_guide_caption"),
                    parse_mode="HTML",
                )
        else:
            await ctx.bot.send_message(
                chat_id=chat_id,
                text=t(chat_id, "watch_guide_text"),
                parse_mode="HTML",
            )
        return

# ==================== Poll Loop ====================

async def poll_loop(app: Application):
    await asyncio.sleep(3)
    logger.info("Poll loop started")

    async with aiohttp.ClientSession() as session:
        while True:
            for chat_id, wallets in list(watched.items()):
                for addr, w in list(wallets.items()):
                    try:
                        positions_raw = await fetch_positions(session, w.address)
                        matches_raw = await fetch_order_matches(session, w.address, first=20)

                        # Both calls failing in the same poll means transient
                        # upstream trouble — count, and notify once on threshold
                        # crossing rather than updating stale snapshot data.
                        if positions_raw is None and matches_raw is None:
                            w.fetch_errors += 1
                            if (
                                w.fetch_errors >= ERROR_ALERT_THRESHOLD
                                and not w.error_notified
                                and not w.muted
                            ):
                                try:
                                    await app.bot.send_message(
                                        chat_id=chat_id,
                                        text=t(
                                            chat_id,
                                            "fetch_error_alert",
                                            count=w.fetch_errors,
                                            addr=fmt_addr(w.address),
                                        ),
                                        parse_mode="HTML",
                                    )
                                    w.error_notified = True
                                except Exception as send_err:
                                    logger.warning(
                                        f"Failed to send fetch-error alert for {addr}: {send_err}"
                                    )
                            await asyncio.sleep(1)
                            continue

                        # At least one call succeeded — reset error state.
                        if w.fetch_errors or w.error_notified:
                            w.fetch_errors = 0
                            w.error_notified = False

                        positions = positions_raw or []
                        matches = matches_raw or []
                        market_ids = {p.get("marketId") for p in positions if p.get("marketId")}
                        markets = {mid: await fetch_market(session, mid) for mid in market_ids}
                        for p in positions:
                            p["_market"] = markets.get(p.get("marketId"), {})
                        positions_by_key = {pos_key(p): p for p in positions}

                        added, changed, closed = diff_positions(w.position_snapshot, positions)
                        new_match_keys = {match_key(m) for m in matches}
                        new_fills = [m for m in matches if match_key(m) not in w.order_match_snapshot]

                        # Refresh the shares snapshot + the title cache. Hold onto
                        # old titles so "closed" notifications can show a real
                        # market name even though the position is gone now.
                        new_titles: dict[str, str] = {}
                        for p in positions:
                            title, _, _, _ = display_fields(p, p.get("_market"))
                            new_titles[pos_key(p)] = title[:80]
                        old_titles = dict(w.position_titles)
                        w.position_titles = new_titles
                        w.position_snapshot = {pos_key(p): pos_size(p) for p in positions}
                        w.last_check = time.time()
                        display_addr = fmt_addr(w.address) + (f" · {w.note}" if w.note else "")

                        if (added or changed or closed) and not w.muted:
                            parts = []
                            for p in added:
                                parts.append(fmt_pos(p, "added", chat_id, markets.get(p.get("marketId"))))
                            for p, prev_size in changed:
                                parts.append(
                                    fmt_pos(
                                        p,
                                        "changed",
                                        chat_id,
                                        markets.get(p.get("marketId")),
                                        prev_shares=prev_size,
                                    )
                                )
                            for k in closed:
                                title = old_titles.get(k) or t(
                                    chat_id, "close_fallback", key=k
                                )
                                parts.append(
                                    f"{t(chat_id, 'fmt_closed')} <b>{_html_escape(title)}</b>"
                                )

                            header = t(chat_id, "poll_header", addr=display_addr)
                            text = header + "\n\n".join(parts[:8])

                            if len(parts) > 8:
                                text += t(chat_id, "fmt_more", count=len(parts) - 8)

                            await app.bot.send_message(
                                chat_id=chat_id,
                                text=text,
                                parse_mode="HTML",
                                disable_web_page_preview=True,
                            )

                        if new_fills and not w.muted:
                            merged_fills = consolidate_fills(new_fills)
                            fill_lines = [
                                fmt_match(m, chat_id, positions_by_key)
                                for m in merged_fills[:5]
                            ]
                            await app.bot.send_message(
                                chat_id=chat_id,
                                text=t(chat_id, "fills_header", addr=display_addr)
                                + "\n\n".join(fill_lines),
                                parse_mode="HTML",
                                disable_web_page_preview=True,
                            )

                        w.order_match_snapshot = set(list(new_match_keys)[:100])
                        save_watch(w)
                    except Exception as e:
                        logger.error(f"Poll error {addr}: {e}")

                    await asyncio.sleep(1)

            await asyncio.sleep(POLL_INTERVAL)

# ==================== Main ====================

async def on_startup(app: Application):
    init_db()
    load_state()
    chats, watches_count = db_counts()
    db_path = Path(SQLITE_PATH).resolve()
    volume_mount = (
        os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
        or os.environ.get("RAILWAY_VOLUME_PATH", "").strip()
    )
    on_volume = bool(volume_mount) and str(db_path).startswith(str(Path(volume_mount).resolve()))
    logger.info(
        "DB path check | path=%s exists=%s parent_writable=%s railway_volume=%s on_volume=%s",
        db_path,
        db_path.exists(),
        os.access(db_path.parent, os.W_OK),
        volume_mount or "-",
        on_volume,
    )
    logger.info(f"SQLite ready at {SQLITE_PATH} | chats={chats} watches={watches_count}")
    if "$" in SQLITE_PATH:
        logger.warning("SQLITE_PATH still contains '$'; check env var expansion in deployment settings")
    if not volume_mount:
        logger.warning("No railway volume env detected; set SQLITE_PATH to a mounted volume to keep data across deploys")
    elif not on_volume:
        logger.warning("SQLite path is not under railway volume mount; data may be lost after redeploy")
    await app.bot.set_my_commands(
        [
            BotCommand("start", "开始 / Start"),
            BotCommand("watch", "监控地址 / Watch wallet(s)"),
            BotCommand("unwatch", "取消监控 / Unwatch wallet"),
            BotCommand("list", "监控列表 / Watch list"),
            BotCommand("pos", "查询持仓 / View positions"),
            BotCommand("orders", "最近成交 / Recent fills"),
            BotCommand("note", "地址备注 / Set remark"),
            BotCommand("mute", "暂停提醒 / Mute wallet"),
            BotCommand("unmute", "恢复提醒 / Unmute wallet"),
            BotCommand("settings", "偏好设置 / Settings"),
            BotCommand("export", "导出监控 / Export watches"),
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
    app.add_handler(CommandHandler("note", cmd_note))
    app.add_handler(CommandHandler("mute", cmd_mute))
    app.add_handler(CommandHandler("unmute", cmd_unmute))
    app.add_handler(CommandHandler("settings", cmd_settings))
    app.add_handler(CommandHandler("export", cmd_export))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("lang", cmd_lang))
    app.add_handler(CallbackQueryHandler(on_callback))

    app.post_init = on_startup

    logger.info("Bot starting...")
    app.run_polling()


if __name__ == "__main__":
    main()
