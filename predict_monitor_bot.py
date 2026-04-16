import os
import asyncio
import logging
import time
import json
import sqlite3
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from dataclasses import dataclass, field

import aiohttp
from telegram import (
    Update,
    BotCommand,
    ForceReply,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
)
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

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

# Categories for the /help browser. Keys are i18n-less identifiers; labels and
# command help strings live in I18N under `help_cat_<id>` and `help_cmd_<name>`.
# Kept in lock-step with set_my_commands so the feature guide only shows what
# the menu bar shows. Handlers for removed commands (/alert /export /mute etc.)
# are still registered so typing them continues to work.
HELP_CATEGORIES: list[tuple[str, list[str]]] = [
    ("watch", ["watch", "unwatch", "list", "stop"]),
    ("query", ["pos", "orders", "hot"]),
    ("other", ["settings", "start"]),
]

# Sliding window (seconds) for the /hot volume ranking.
MARKET_FLOW_WINDOW_S = 3600


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
    # Suppress position-change notifications where the share delta is smaller
    # than this percent (0 disables the filter).
    change_threshold_pct: float = 0.0
    # Per-wallet poll interval in seconds (0 = use global POLL_INTERVAL).
    poll_interval_s: int = 0
    # Wall-clock of the last real activity (emitted notification). Powers the
    # "stale" badge in /list.
    last_activity: float = 0
    # Cache of market resolution state keyed by market_id so we only alert
    # once on resolution transitions.
    resolved_markets: dict = field(default_factory=dict)
    # Transient runtime counters (not persisted).
    fetch_errors: int = 0
    error_notified: bool = False


watched: dict[int, dict[str, WatchedWallet]] = {}
market_cache: dict[str, dict] = {}
# market_id -> deque of (ts_epoch, usd_value, chat_id). Populated from fills
# observed in poll_loop; drives the /hot ranking. Not persisted — a 1h window
# refills quickly after restart.
market_flow: dict[str, deque] = defaultdict(lambda: deque(maxlen=2000))
# Titles we've seen for a market while indexing flow, so /hot can render a
# name even if fetch_market hasn't been called for it yet.
market_flow_titles: dict[str, str] = {}
chat_lang: dict[int, str] = {}
# Per-chat notification mode: "split" (default — one message per block) or
# "merged" (legacy T13 — position changes + fills + resolution joined by
# ───── into a single Telegram message).
chat_notify: dict[int, str] = {}
NOTIFY_MODES = ("split", "merged")
db_lock = threading.Lock()

I18N = {
    "en": {
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
        "watch_list": (
            "<b>Watch List</b>\n"
            "<i>💡 Tap a row button: 📊 positions · 📜 fills · ✏️ edit note · "
            "🔕/🔔 mute toggle · 🛑 unwatch.</i>\n"
        ),
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
        "btn_unwatch": "🛑 Unwatch",
        "btn_mute": "🔕 Mute",
        "btn_unmute": "🔔 Unmute",
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
        "settings_interval": "⏱ Poll interval: {interval}s (global)",
        "settings_watches": "👁 Watches: {count} ({muted} muted)",
        "settings_notify": "🔔 Notify mode: {mode}",
        "notify_split": "split (one block per message)",
        "notify_merged": "merged (all blocks in one message with ─────)",
        "btn_notify_split": "🔔 Split messages",
        "btn_notify_merged": "🧩 Merge into one",
        "notify_switched": "Notification mode set to: {mode}",
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
        "start": (
            "<b>Predict.fun Monitor Bot</b>\n\n"
            "/help - feature guide\n"
            "/watch <code>0xAddr ...</code> - monitor one or more wallets\n"
            "/unwatch <code>addr|alias</code> - stop watching\n"
            "/list - watched list\n"
            "/pos <code>addr|alias</code> - view positions\n"
            "/orders <code>addr|alias</code> - recent fills\n"
            "/settings - preferences\n"
            "/stop - stop all"
        ),
        # --- PnL, sorting, portfolio ---
        "label_pnl": "P&L",
        "label_total_pnl": "Total P&L",
        "label_sorted_by": "sorted by",
        "label_largest_pos": "Largest",
        "sort_value": "value",
        "sort_size": "size",
        "sort_alpha": "alpha",
        "btn_sort": "↕ Sort: {label}",
        "btn_portfolio": "📊 Portfolio card",
        "btn_hide_portfolio": "⬅ Hide card",
        "portfolio_title": "<b>Portfolio · {addr}</b>",
        "portfolio_capital": "💰 Deployed: <b>${amount:,.2f}</b>",
        "portfolio_positions": "📦 Positions: <b>{count}</b>",
        "portfolio_pnl": "📈 Unrealized P&L: {arrow} <b>{sign}${amount:,.2f}</b>",
        "portfolio_largest": "🏆 Largest: <b>{title}</b> (${amount:,.2f})",
        # --- Resolution ---
        "resolve_win": "🏆 你的仓位命中了（held outcome won）",
        "resolve_lose": "💀 你的仓位未命中（held outcome lost）",
        "label_resolve_outcome": "Winning outcome",
        "poll_resolve_header": "🏁 <b>Market resolved</b> <code>{addr}</code>\n\n",
        # --- Threshold / interval ---
        "usage_threshold": "Usage: /threshold addr|alias pct",
        "threshold_set": "🎚 Min-change filter for <code>{addr}</code>: {pct}%",
        "usage_interval": "Usage: /interval addr|alias seconds (0 = global)",
        "interval_set": "⏱ Poll interval for <code>{addr}</code>: {sec}s",
        "interval_too_small": "Interval must be 0 (global) or ≥ 5 seconds.",
        # --- Alerts ---
        "usage_alert": "Usage: /alert addr|alias outcome >=|<= price(0-100)",
        "alert_bad_outcome": "Outcome not found for this wallet's position.",
        "alert_bad_op": "Operator must be >= or <=.",
        "alert_bad_price": "Price must be between 0 and 100 (cents).",
        "alert_created": "🔔 Alert #{id} set: {outcome} {op} {threshold:.2f}c",
        "alerts_empty": "No alerts.",
        "alerts_header": "<b>Alerts ({count})</b>\n\n",
        "alerts_line": "#{id} · {outcome} {op} {threshold:.2f}c · {title}",
        "usage_unalert": "Usage: /unalert id",
        "unalert_not_found": "Alert not found.",
        "unalert_ok": "🗑 Removed alert #{id}.",
        "alert_fired": "🔔 <b>Price alert</b> <code>{addr}</code>\n\n{title}\n{outcome} {op} {threshold:.2f}c · now <b>{price:.2f}c</b>",
        # --- Import / export ---
        "import_no_file": "Reply to a JSON document with /import to bulk-add wallets.",
        "import_done": "Imported {added} new · {skipped} skipped (dup/invalid).",
        "import_parse_error": "Could not parse the JSON file.",
        "import_too_big": "File too large (max 256 KB).",
        # --- Duplicate-watch handling ---
        "btn_overwrite_note": "✏️ Overwrite note",
        "btn_cancel": "Cancel",
        "dup_watch_prompt": "<code>{addr}</code> is already watched with note <b>{old}</b>. Overwrite with <b>{new}</b>?",
        "dup_watch_overwritten": "Note overwritten.",
        "dup_watch_cancelled": "Cancelled.",
        # --- /list stale badge & fresh badge ---
        "label_stale_flag": "💤 dormant",
        "label_fresh_flag": "🌱 just added",
        # --- Recovery ---
        "api_recovered": "✅ Predict.fun API recovered for <code>{addr}</code>.",
        # --- Note-edit flow ---
        "btn_edit_note": "✏️ Note",
        "edit_note_prompt": "Reply with the new note for <code>{addr}</code> (send empty to clear):",
        "edit_note_saved": "Note for <code>{addr}</code> → {note}",
        "watch_example": (
            "💡 <b>Format examples</b>\n"
            "<code>/watch 0x1234…abcd</code>  — single wallet\n"
            "<code>/watch 0x1234…abcd alice</code>  — with note\n"
            "<code>/watch 0xAAA 0xBBB 0xCCC</code>  — multiple"
        ),
        # --- /help browser ---
        "help_title": "📖 <b>Feature guide</b>\n\nPick a category to browse commands:",
        "help_cmd_title": "<b>{cmd}</b>\n\n",
        "btn_back": "⬅ Back",
        "btn_help": "📖 Feature guide",
        "help_cat_watch": "📡 Watch management",
        "help_cat_query": "🔎 Queries",
        "help_cat_alert": "🔔 Alerts",
        "help_cat_filter": "🎚 Filters",
        "help_cat_other": "⚙ Other",
        "help_cat_header_watch": "📡 <b>Watch management</b>\n\n",
        "help_cat_header_query": "🔎 <b>Queries</b>\n\n",
        "help_cat_header_alert": "🔔 <b>Alerts</b>\n\n",
        "help_cat_header_filter": "🎚 <b>Filters</b>\n\n",
        "help_cat_header_other": "⚙ <b>Other</b>\n\n",
        # Per-command help blurbs.
        "help_cmd_start": "<b>/start</b> — Welcome screen\n\nShows the welcome message, language picker, and the wallet-discovery guide.",
        "help_cmd_watch": (
            "<b>/watch</b> — Start monitoring one or more wallets\n\n"
            "Subscribes to position changes, new fills and market resolutions for the given wallet(s). "
            "Calling /watch without arguments opens the wallet-discovery guide.\n\n"
            "<b>Usage</b>\n"
            "• <code>/watch 0x1234…abcd</code>\n"
            "• <code>/watch 0x1234…abcd alice</code>  (with note)\n"
            "• <code>/watch 0xAAA 0xBBB 0xCCC</code>  (bulk)"
        ),
        "help_cmd_unwatch": (
            "<b>/unwatch</b> — Stop watching a wallet\n\n"
            "Accepts either the raw address or its note (alias).\n\n"
            "<b>Usage</b>\n"
            "• <code>/unwatch 0x1234…abcd</code>\n"
            "• <code>/unwatch alice</code>"
        ),
        "help_cmd_list": (
            "<b>/list</b> — Watched wallets\n\n"
            "One row per wallet with full address, note, position count and last-check time. "
            "Each row has inline buttons for 📊 positions, 📜 fills, ✏️ edit-note, 🔕/🔔 mute toggle and 🛑 unwatch. "
            "Paginates at 8 wallets per page."
        ),
        "help_cmd_pos": (
            "<b>/pos</b> — Positions for a wallet\n\n"
            "Shows current holdings with cost basis, mark price, unrealized P&L per position and in total. "
            "Buttons let you refresh, switch to fills, change sort order (value / size / alpha), "
            "or expand the portfolio summary card.\n\n"
            "<b>Usage</b>\n"
            "• <code>/pos 0x1234…abcd</code>\n"
            "• <code>/pos alice</code>"
        ),
        "help_cmd_portfolio": (
            "<b>/portfolio</b> — Portfolio summary card\n\n"
            "Same as /pos but opens with the portfolio card expanded (deployed capital, largest holding, total P&L). "
            "You can also reach it from the 📊 Portfolio card button inside /pos."
        ),
        "help_cmd_orders": (
            "<b>/orders</b> — Recent fills\n\n"
            "Up to 30 fills paginated 8 per page. Partial fills of the same order are merged into one line with a "
            "(N fills) suffix. Each fill shows the executed price, USD value, the wallet's total holding in that market, "
            "and a link to the on-chain transaction."
        ),
        "help_cmd_hot": (
            "<b>/hot</b> — Top markets by 1h volume\n\n"
            "Ranks markets by USD volume observed in the last hour from fills across the monitored wallets. "
            "Results are estimated from the bot's own order-flow stream (not the whole site).\n\n"
            "<b>Usage</b>\n"
            "• <code>/hot</code> — all observed markets\n"
            "• <code>/hot mine</code> — only markets your watched wallets traded"
        ),
        "hot_header": "🔥 <b>Top markets · last 1h</b> ({scope})",
        "hot_line": "{rank}. {title}\n   💵 <b>${usd:,.2f}</b> · {count} fills",
        "hot_empty": "No fills observed in the last hour yet. Watch more wallets or wait a bit.",
        "hot_scope_all": "all watched wallets",
        "hot_scope_mine": "your watched wallets",
        "help_cmd_note": (
            "<b>/note</b> — Set a note / alias\n\n"
            "After setting, you can use the note instead of the full 0x address in every other command.\n\n"
            "<b>Usage</b>\n"
            "• <code>/note 0x1234…abcd alice</code>\n\n"
            "💡 Tip: in /list you can tap the ✏️ button next to a wallet to edit its note inline."
        ),
        "help_cmd_mute": (
            "<b>/mute</b> — Silence alerts for a wallet\n\n"
            "The wallet is still polled, but position/fill notifications are suppressed. Price alerts still fire.\n\n"
            "<b>Usage</b>\n• <code>/mute alice</code>\n\n"
            "💡 Tip: the 🔕 button in /list toggles mute."
        ),
        "help_cmd_unmute": (
            "<b>/unmute</b> — Resume alerts for a wallet\n\n"
            "<b>Usage</b>\n• <code>/unmute alice</code>\n\n"
            "💡 Tip: the 🔔 button in /list toggles unmute."
        ),
        "help_cmd_stop": (
            "<b>/stop</b> — Remove every watched wallet\n\n"
            "Nukes all monitoring for this chat. Price alerts for those wallets also go away."
        ),
        "help_cmd_alert": (
            "<b>/alert</b> — Set a one-shot price alert\n\n"
            "Fires a single notification when the market price of the wallet's position in that outcome crosses a "
            "threshold. The rule is automatically deleted after firing.\n\n"
            "<b>Usage</b>\n"
            "• <code>/alert alice YES &gt;= 80</code>  (in cents, 0–100)\n"
            "• <code>/alert 0xAddr NO &lt;= 25</code>"
        ),
        "help_cmd_alerts": (
            "<b>/alerts</b> — List active alerts\n\n"
            "<b>Usage</b>\n"
            "• <code>/alerts</code>  (all alerts)\n"
            "• <code>/alerts alice</code>  (alerts for one wallet)"
        ),
        "help_cmd_unalert": (
            "<b>/unalert</b> — Delete an alert by id\n\n"
            "Get the id from /alerts.\n\n"
            "<b>Usage</b>\n• <code>/unalert 42</code>"
        ),
        "help_cmd_threshold": (
            "<b>/threshold</b> — Min-change filter per wallet\n\n"
            "Suppresses position-change notifications whose share delta is smaller than the given percent. "
            "0 disables the filter (default).\n\n"
            "<b>Usage</b>\n• <code>/threshold alice 5</code>  (ignore &lt;5% rebalances)"
        ),
        "help_cmd_interval": (
            "<b>/interval</b> — Per-wallet poll interval\n\n"
            "0 (default) means use the global interval. Minimum when set is 5 seconds.\n\n"
            "<b>Usage</b>\n• <code>/interval alice 60</code>  (poll every 60s)"
        ),
        "help_cmd_settings": (
            "<b>/settings</b> — Preferences card\n\n"
            "Shows current language, global poll interval, watch count and muted count. "
            "Language buttons toggle EN/中文 in place."
        ),
        "help_cmd_lang": (
            "<b>/lang</b> — Switch bot language\n\n"
            "<b>Usage</b>\n"
            "• <code>/lang en</code>\n"
            "• <code>/lang zh</code>\n\n"
            "💡 Tip: /start and /settings both have language buttons."
        ),
        "help_cmd_export": (
            "<b>/export</b> — Download your watch list\n\n"
            "Emits a <code>predict_watches.json</code> file with every watched address, note and muted flag. "
            "Use /import to restore it later."
        ),
        "help_cmd_import": (
            "<b>/import</b> — Bulk-import a watch list\n\n"
            "Reply to a <code>predict_watches.json</code> file (or any JSON document with the same shape) with /import "
            "to add the wallets in bulk. Duplicates are skipped. Max size 256 KB."
        ),
    },
    "zh": {
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
        "watch_list": (
            "<b>监控列表</b>\n"
            "<i>💡 每行按钮：📊 查看持仓 · 📜 查看成交 · ✏️ 编辑备注 · "
            "🔕/🔔 静音切换 · 🛑 取消监控。</i>\n"
        ),
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
        "btn_unwatch": "🛑 取消",
        "btn_mute": "🔕 静音",
        "btn_unmute": "🔔 提醒",
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
        "settings_interval": "⏱ 轮询间隔：{interval} 秒（全局）",
        "settings_watches": "👁 监控数：{count}（已静音 {muted}）",
        "settings_notify": "🔔 通知模式：{mode}",
        "notify_split": "分开发（每类事件一条消息）",
        "notify_merged": "合并发（一条消息里用 ───── 拼接）",
        "btn_notify_split": "🔔 分开发",
        "btn_notify_merged": "🧩 合并发",
        "notify_switched": "通知模式已设为：{mode}",
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
        "start": (
            "<b>Predict.fun 监控机器人</b>\n\n"
            "/help - 功能介绍\n"
            "/watch <code>0xAddr ...</code> - 监控钱包，可一次多个\n"
            "/unwatch <code>地址或备注</code> - 停止监控\n"
            "/list - 查看监控列表\n"
            "/pos <code>地址或备注</code> - 查看持仓\n"
            "/orders <code>地址或备注</code> - 查看最近成交\n"
            "/settings - 偏好设置\n"
            "/stop - 清空全部监控"
        ),
        "label_pnl": "浮动盈亏",
        "label_total_pnl": "总浮动盈亏",
        "label_sorted_by": "排序",
        "label_largest_pos": "最大仓",
        "sort_value": "价值",
        "sort_size": "数量",
        "sort_alpha": "字母",
        "btn_sort": "↕ 排序：{label}",
        "btn_portfolio": "📊 汇总卡片",
        "btn_hide_portfolio": "⬅ 收起",
        "portfolio_title": "<b>组合 · {addr}</b>",
        "portfolio_capital": "💰 已投入：<b>${amount:,.2f}</b>",
        "portfolio_positions": "📦 持仓数：<b>{count}</b>",
        "portfolio_pnl": "📈 浮动盈亏：{arrow} <b>{sign}${amount:,.2f}</b>",
        "portfolio_largest": "🏆 最大仓：<b>{title}</b>（${amount:,.2f}）",
        "resolve_win": "🏆 持仓方向命中",
        "resolve_lose": "💀 持仓方向未命中",
        "label_resolve_outcome": "获胜结果",
        "poll_resolve_header": "🏁 <b>市场已结算</b> <code>{addr}</code>\n\n",
        "usage_threshold": "用法：/threshold 地址或备注 百分比",
        "threshold_set": "🎚 <code>{addr}</code> 最小变动过滤：{pct}%",
        "usage_interval": "用法：/interval 地址或备注 秒（0 = 使用全局）",
        "interval_set": "⏱ <code>{addr}</code> 轮询间隔：{sec} 秒",
        "interval_too_small": "间隔必须为 0（使用全局）或 ≥ 5 秒。",
        "usage_alert": "用法：/alert 地址或备注 结果名 >=|<= 价格(0-100)",
        "alert_bad_outcome": "该钱包当前持仓中未找到该结果。",
        "alert_bad_op": "运算符只能是 >= 或 <=。",
        "alert_bad_price": "价格必须是 0~100（cents）。",
        "alert_created": "🔔 已创建警报 #{id}：{outcome} {op} {threshold:.2f}c",
        "alerts_empty": "暂无警报。",
        "alerts_header": "<b>警报（{count}）</b>\n\n",
        "alerts_line": "#{id} · {outcome} {op} {threshold:.2f}c · {title}",
        "usage_unalert": "用法：/unalert id",
        "unalert_not_found": "未找到该警报。",
        "unalert_ok": "🗑 已删除警报 #{id}。",
        "alert_fired": "🔔 <b>价格警报</b> <code>{addr}</code>\n\n{title}\n{outcome} {op} {threshold:.2f}c · 当前 <b>{price:.2f}c</b>",
        "import_no_file": "请回复一个 JSON 文件并附带 /import 来批量导入钱包。",
        "import_done": "导入完成：新增 {added} · 跳过 {skipped}（重复或无效）。",
        "import_parse_error": "JSON 文件解析失败。",
        "import_too_big": "文件过大（最大 256 KB）。",
        "btn_overwrite_note": "✏️ 覆盖备注",
        "btn_cancel": "取消",
        "dup_watch_prompt": "<code>{addr}</code> 已在监控，当前备注 <b>{old}</b>。要覆盖为 <b>{new}</b> 吗？",
        "dup_watch_overwritten": "备注已覆盖。",
        "dup_watch_cancelled": "已取消。",
        "label_stale_flag": "💤 休眠",
        "label_fresh_flag": "🌱 刚添加",
        "api_recovered": "✅ <code>{addr}</code> 的 Predict.fun API 已恢复。",
        "btn_edit_note": "✏️ 备注",
        "edit_note_prompt": "回复此消息以设置 <code>{addr}</code> 的新备注（发空即清除）：",
        "edit_note_saved": "<code>{addr}</code> 备注已更新为：{note}",
        "watch_example": (
            "💡 <b>格式示例</b>\n"
            "<code>/watch 0x1234…abcd</code>  — 单个钱包\n"
            "<code>/watch 0x1234…abcd 张三</code>  — 带备注\n"
            "<code>/watch 0xAAA 0xBBB 0xCCC</code>  — 批量"
        ),
        # --- /help 浏览器 ---
        "help_title": "📖 <b>功能介绍</b>\n\n请选择分类，查看命令详情：",
        "help_cmd_title": "<b>{cmd}</b>\n\n",
        "btn_back": "⬅ 返回",
        "btn_help": "📖 功能介绍",
        "help_cat_watch": "📡 监控管理",
        "help_cat_query": "🔎 查询",
        "help_cat_alert": "🔔 警报",
        "help_cat_filter": "🎚 过滤",
        "help_cat_other": "⚙ 其他",
        "help_cat_header_watch": "📡 <b>监控管理</b>\n\n",
        "help_cat_header_query": "🔎 <b>查询</b>\n\n",
        "help_cat_header_alert": "🔔 <b>警报</b>\n\n",
        "help_cat_header_filter": "🎚 <b>过滤</b>\n\n",
        "help_cat_header_other": "⚙ <b>其他</b>\n\n",
        "help_cmd_start": "<b>/start</b> — 欢迎页\n\n显示欢迎信息、语言切换，以及「如何找到钱包地址」图文指南。",
        "help_cmd_watch": (
            "<b>/watch</b> — 开始监控钱包\n\n"
            "对给定地址订阅持仓变化、新成交和市场结算通知。不带参数调用会直接弹出「如何找到钱包地址」指南。\n\n"
            "<b>用法</b>\n"
            "• <code>/watch 0x1234…abcd</code>\n"
            "• <code>/watch 0x1234…abcd 张三</code>（带备注）\n"
            "• <code>/watch 0xAAA 0xBBB 0xCCC</code>（批量）"
        ),
        "help_cmd_unwatch": (
            "<b>/unwatch</b> — 取消监控某个钱包\n\n"
            "地址或备注都可以。\n\n"
            "<b>用法</b>\n"
            "• <code>/unwatch 0x1234…abcd</code>\n"
            "• <code>/unwatch 张三</code>"
        ),
        "help_cmd_list": (
            "<b>/list</b> — 查看所有被监控的钱包\n\n"
            "每行显示完整地址、备注、持仓数和最后检查时间。每行配 📊 持仓 / 📜 成交 / ✏️ 编辑备注 / 🔕🔔 静音切换 / 🛑 取消监控 按钮，超过 8 个自动分页。"
        ),
        "help_cmd_pos": (
            "<b>/pos</b> — 查询钱包当前持仓\n\n"
            "显示每个仓位的成本、现价、浮动盈亏，以及总盈亏。按钮支持 🔄 刷新 / 📜 查看成交 / ↕ 切换排序（价值/数量/字母）/ 📊 汇总卡片。\n\n"
            "<b>用法</b>\n"
            "• <code>/pos 0x1234…abcd</code>\n"
            "• <code>/pos 张三</code>"
        ),
        "help_cmd_portfolio": (
            "<b>/portfolio</b> — 持仓汇总卡片\n\n"
            "和 /pos 功能一样，但默认展开「汇总卡片」：已投入、最大仓、总浮动盈亏。也可以从 /pos 的「📊 汇总卡片」按钮进入。"
        ),
        "help_cmd_orders": (
            "<b>/orders</b> — 最近成交\n\n"
            "最多 30 条，每页 8 条翻页。相同订单的分段成交合并成一条（带「共 N 笔」后缀）。每条显示成交价格、USD 价值、钱包在该市场的总持仓以及链上交易哈希。"
        ),
        "help_cmd_hot": (
            "<b>/hot</b> — 近 1 小时市场成交量排行\n\n"
            "按近 1 小时内观察到的 USD 成交额给市场排名。数据来源是机器人自己拉到的成交流（监控钱包的 fills），不是 Predict.fun 全站真实量。\n\n"
            "<b>用法</b>\n"
            "• <code>/hot</code> — 所有观察到的市场\n"
            "• <code>/hot mine</code> — 只看本聊天监控的钱包成交的市场"
        ),
        "hot_header": "🔥 <b>近 1 小时成交量排行</b>（{scope}）",
        "hot_line": "{rank}. {title}\n   💵 <b>${usd:,.2f}</b> · {count} 笔成交",
        "hot_empty": "近 1 小时还没有观察到成交。多监控几个钱包或稍等一会再试。",
        "hot_scope_all": "全部监控钱包",
        "hot_scope_mine": "本聊天监控的钱包",
        "help_cmd_note": (
            "<b>/note</b> — 设置地址备注/别名\n\n"
            "设置后，所有命令里都可以用备注代替地址输入。\n\n"
            "<b>用法</b>\n• <code>/note 0x1234…abcd 张三</code>\n\n"
            "💡 提示：在 /list 里点 ✏️ 按钮可以直接就地修改备注。"
        ),
        "help_cmd_mute": (
            "<b>/mute</b> — 暂停某钱包的提醒\n\n"
            "轮询继续，但持仓/成交的通知会被压掉。价格警报仍然生效。\n\n"
            "<b>用法</b>\n• <code>/mute 张三</code>\n\n"
            "💡 提示：/list 里 🔕 按钮可以一键切换。"
        ),
        "help_cmd_unmute": (
            "<b>/unmute</b> — 恢复提醒\n\n"
            "<b>用法</b>\n• <code>/unmute 张三</code>\n\n"
            "💡 提示：/list 里 🔔 按钮可以一键切换。"
        ),
        "help_cmd_stop": (
            "<b>/stop</b> — 清空当前会话的全部监控\n\n"
            "会一并删掉这些钱包的价格警报。请谨慎使用。"
        ),
        "help_cmd_alert": (
            "<b>/alert</b> — 单次价格警报\n\n"
            "当指定钱包在指定结果上的市场价跨越阈值时推送一次通知，然后自动删除。\n\n"
            "<b>用法</b>\n"
            "• <code>/alert 张三 YES &gt;= 80</code>（cents，0–100）\n"
            "• <code>/alert 0x地址 NO &lt;= 25</code>"
        ),
        "help_cmd_alerts": (
            "<b>/alerts</b> — 查看已设置的警报\n\n"
            "<b>用法</b>\n"
            "• <code>/alerts</code>（全部）\n"
            "• <code>/alerts 张三</code>（单个钱包）"
        ),
        "help_cmd_unalert": (
            "<b>/unalert</b> — 按 id 删除警报\n\n"
            "id 可以从 /alerts 的输出里找到。\n\n"
            "<b>用法</b>\n• <code>/unalert 42</code>"
        ),
        "help_cmd_threshold": (
            "<b>/threshold</b> — 单钱包最小变动过滤\n\n"
            "份额变动小于设定百分比的通知会被丢掉。0 = 关闭过滤（默认）。\n\n"
            "<b>用法</b>\n• <code>/threshold 张三 5</code>（&lt; 5% 的调仓不提醒）"
        ),
        "help_cmd_interval": (
            "<b>/interval</b> — 单钱包独立轮询间隔\n\n"
            "0（默认）使用全局间隔。设置时最小 5 秒。\n\n"
            "<b>用法</b>\n• <code>/interval 张三 60</code>（60 秒一次）"
        ),
        "help_cmd_settings": (
            "<b>/settings</b> — 偏好面板\n\n"
            "显示当前语言、全局轮询间隔、监控数和静音数，下面有语言切换按钮。"
        ),
        "help_cmd_lang": (
            "<b>/lang</b> — 切换机器人语言\n\n"
            "<b>用法</b>\n"
            "• <code>/lang zh</code>\n"
            "• <code>/lang en</code>\n\n"
            "💡 提示：/start 和 /settings 里都有 🇺🇸/🇨🇳 按钮可以切换。"
        ),
        "help_cmd_export": (
            "<b>/export</b> — 导出监控列表\n\n"
            "生成 <code>predict_watches.json</code>，包含每个钱包的地址、备注、静音状态。可以用 /import 恢复。"
        ),
        "help_cmd_import": (
            "<b>/import</b> — 批量导入监控列表\n\n"
            "回复一个 <code>predict_watches.json</code> 文件（或相同格式的 JSON），附带 /import 即可批量添加。重复的会跳过，最大 256 KB。"
        ),
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
                change_threshold_pct REAL NOT NULL DEFAULT 0,
                poll_interval_s INTEGER NOT NULL DEFAULT 0,
                last_activity REAL NOT NULL DEFAULT 0,
                resolved_markets TEXT NOT NULL DEFAULT '{}',
                PRIMARY KEY (chat_id, address)
            )
            """
        )
        # Tolerate upgrades from older schemas that pre-date these columns.
        for ddl in (
            "ALTER TABLE watches ADD COLUMN muted INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE watches ADD COLUMN position_titles TEXT NOT NULL DEFAULT '{}'",
            "ALTER TABLE watches ADD COLUMN change_threshold_pct REAL NOT NULL DEFAULT 0",
            "ALTER TABLE watches ADD COLUMN poll_interval_s INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE watches ADD COLUMN last_activity REAL NOT NULL DEFAULT 0",
            "ALTER TABLE watches ADD COLUMN resolved_markets TEXT NOT NULL DEFAULT '{}'",
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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_notify (
                chat_id INTEGER PRIMARY KEY,
                mode TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                address TEXT NOT NULL,
                market_id TEXT NOT NULL,
                outcome_index INTEGER NOT NULL,
                op TEXT NOT NULL,
                threshold REAL NOT NULL,
                created_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS alerts_by_chat ON alerts(chat_id, address)"
        )
        conn.commit()


def save_watch(w: WatchedWallet):
    with db_lock, db_conn() as conn:
        conn.execute(
            """
            INSERT INTO watches(
              chat_id, address, note, position_snapshot, order_match_snapshot,
              last_check, muted, position_titles, change_threshold_pct,
              poll_interval_s, last_activity, resolved_markets
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(chat_id, address) DO UPDATE SET
              note=excluded.note,
              position_snapshot=excluded.position_snapshot,
              order_match_snapshot=excluded.order_match_snapshot,
              last_check=excluded.last_check,
              muted=excluded.muted,
              position_titles=excluded.position_titles,
              change_threshold_pct=excluded.change_threshold_pct,
              poll_interval_s=excluded.poll_interval_s,
              last_activity=excluded.last_activity,
              resolved_markets=excluded.resolved_markets
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
                float(w.change_threshold_pct or 0),
                int(w.poll_interval_s or 0),
                float(w.last_activity or 0),
                json.dumps(w.resolved_markets, ensure_ascii=False),
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


def save_chat_notify(chat_id: int, mode: str):
    with db_lock, db_conn() as conn:
        conn.execute(
            """
            INSERT INTO chat_notify(chat_id, mode) VALUES(?,?)
            ON CONFLICT(chat_id) DO UPDATE SET mode=excluded.mode
            """,
            (chat_id, mode),
        )
        conn.commit()


# --- alerts + events persistence -----------------------------------------


def insert_alert(
    chat_id: int,
    address: str,
    market_id: str,
    outcome_index: int,
    op: str,
    threshold_cents: float,
) -> int:
    """Persist a new price alert and return its id."""
    with db_lock, db_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO alerts(chat_id, address, market_id, outcome_index, op, threshold, created_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                chat_id,
                address,
                market_id,
                int(outcome_index),
                op,
                float(threshold_cents),
                time.time(),
            ),
        )
        conn.commit()
        return int(cur.lastrowid or 0)


def list_alerts(chat_id: int, address: str | None = None) -> list[dict]:
    query = "SELECT id, chat_id, address, market_id, outcome_index, op, threshold, created_at FROM alerts WHERE chat_id=?"
    args: list = [chat_id]
    if address:
        query += " AND lower(address)=lower(?)"
        args.append(address)
    query += " ORDER BY created_at ASC"
    with db_lock, db_conn() as conn:
        rows = conn.execute(query, args).fetchall()
    return [
        {
            "id": r[0],
            "chat_id": r[1],
            "address": r[2],
            "market_id": r[3],
            "outcome_index": r[4],
            "op": r[5],
            "threshold": r[6],
            "created_at": r[7],
        }
        for r in rows
    ]


def delete_alert(chat_id: int, alert_id: int) -> bool:
    with db_lock, db_conn() as conn:
        cur = conn.execute(
            "DELETE FROM alerts WHERE chat_id=? AND id=?", (chat_id, alert_id)
        )
        conn.commit()
        return cur.rowcount > 0


def load_state():
    with db_lock, db_conn() as conn:
        for chat_id, lang in conn.execute("SELECT chat_id, lang FROM chat_lang"):
            chat_lang[int(chat_id)] = lang

        try:
            for chat_id, mode in conn.execute("SELECT chat_id, mode FROM chat_notify"):
                if mode in NOTIFY_MODES:
                    chat_notify[int(chat_id)] = mode
        except sqlite3.OperationalError:
            # Upgrading from a pre-chat_notify DB; init_db will have created
            # the table by the time we're here, so this should be rare.
            pass

        for row in conn.execute(
            """
            SELECT chat_id, address, note, position_snapshot, order_match_snapshot,
                   last_check, muted, position_titles, change_threshold_pct,
                   poll_interval_s, last_activity, resolved_markets
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
                change_threshold_pct,
                poll_interval_s,
                last_activity,
                resolved_json,
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
                change_threshold_pct=float(change_threshold_pct or 0),
                poll_interval_s=int(poll_interval_s or 0),
                last_activity=float(last_activity or 0),
                resolved_markets=json.loads(resolved_json or "{}"),
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


def mark_price_cents(pos: dict, market: dict | None = None) -> float | None:
    """Return the current market (mark) price in cents for a position.

    Distinct from entry/avg price in `display_fields`, which is what the user
    paid. The mark price drives unrealized P&L and price-alert triggers.
    """
    market = market or {}
    outcome_obj = pos.get("outcome") if isinstance(pos.get("outcome"), dict) else {}
    market_obj = pos.get("market") if isinstance(pos.get("market"), dict) else {}

    raw = (
        pos.get("currentPrice")
        or outcome_obj.get("price")
        or outcome_obj.get("currentPrice")
    )
    if raw is None:
        idx = pos.get("outcomeIndex")
        if idx is None:
            idx = outcome_obj.get("index")
        if idx is not None:
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
                    raw = prices[i]
            except (ValueError, TypeError):
                pass

    return _norm_price_to_cents(raw)


def pnl_of(pos: dict, market: dict | None = None) -> tuple[float | None, float | None, float | None]:
    """Compute unrealized PnL for a position.

    Returns ``(pnl_usd, pnl_pct, mark_price_cents)``. Any component can be
    ``None`` when the underlying data is missing (no mark price, zero entry).
    """
    shares_num = _norm_amount_to_shares(
        pos.get("shares") or pos.get("quantity") or pos.get("amount")
    )
    if not shares_num or shares_num <= 0:
        return None, None, None

    # Re-use the same entry-price resolution as display_fields so PnL is
    # computed against the user's cost basis, not the live mark.
    _, _, _, entry_text = display_fields(pos, market)
    try:
        entry_c = float(entry_text)
    except (ValueError, TypeError):
        entry_c = None

    mark_c = mark_price_cents(pos, market)
    if entry_c is None or mark_c is None:
        return None, None, mark_c

    pnl_usd = shares_num * (mark_c - entry_c) / 100
    pnl_pct = ((mark_c - entry_c) / entry_c * 100) if entry_c > 0 else None
    return pnl_usd, pnl_pct, mark_c


def market_resolution(market: dict | None) -> tuple[bool, int | None]:
    """Detect whether a market is resolved and, if so, which outcome won.

    Returns ``(is_resolved, winning_index_or_None)``.
    """
    if not isinstance(market, dict):
        return False, None
    status = str(market.get("status") or market.get("state") or "").lower()
    resolved = bool(market.get("resolved")) or status in ("resolved", "closed", "settled")
    winning = market.get("resolvedOutcome")
    if winning is None:
        winning = market.get("winningOutcome")
    if winning is None:
        winning = market.get("winningOutcomeIndex")
    try:
        winning_idx = int(winning) if winning is not None else None
    except (ValueError, TypeError):
        winning_idx = None
    return resolved, winning_idx


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


def diff_positions(
    old: dict,
    new_list: list[dict],
    threshold_pct: float = 0.0,
):
    """Compute added/changed/closed positions.

    For ``changed`` entries we return ``(position, previous_shares)`` so the
    notification can display the delta (e.g. "5 → 12"). ``previous_shares`` is
    ``None`` when migrating from the legacy hash-based snapshot.

    ``threshold_pct`` (0–100) suppresses small share-count changes: a change
    that represents less than this percent of the previous size won't be
    reported. ``0`` disables the filter (default).
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
        if abs(new_val - old_val) <= 1e-9:
            continue
        if threshold_pct and old_val:
            rel = abs(new_val - old_val) / abs(old_val) * 100.0
            if rel < threshold_pct:
                continue
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


def _pnl_line(chat_id: int, pnl_usd: float | None, pnl_pct: float | None) -> str:
    if pnl_usd is None or pnl_pct is None:
        return ""
    arrow = "🟢" if pnl_usd > 0 else ("🔴" if pnl_usd < 0 else "⚪")
    sign = "+" if pnl_usd >= 0 else "−"
    return (
        f"\n📈 {t(chat_id, 'label_pnl')}: {arrow} "
        f"<b>{sign}${abs(pnl_usd):,.2f}</b> ({sign}{abs(pnl_pct):.2f}%)"
    )


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
    pnl_usd, pnl_pct, _ = pnl_of(pos, combined_market)
    pnl_line = _pnl_line(chat_id, pnl_usd, pnl_pct) if label != "closed" else ""

    return (
        f"{emoji} <b>{_title_html(title, combined_market)}</b>\n"
        f"✅ <b>{outcome}</b>\n"
        f"{size_line}\n"
        f"💰 {t(chat_id, 'label_value')}: <b>{val}</b>"
        f"{pnl_line}"
    )


def fmt_resolution(
    chat_id: int,
    market: dict | None,
    winning_outcome: int | None,
    held_outcome: int | None,
) -> str:
    title = "?"
    if isinstance(market, dict):
        title = market.get("title") or market.get("question") or market.get("id") or "?"
    outcomes = []
    if isinstance(market, dict):
        outcomes = market.get("outcomes") or market.get("outcomeNames") or []

    def _outcome_name(idx):
        if idx is None:
            return "?"
        try:
            i = int(idx)
            if isinstance(outcomes, list) and 0 <= i < len(outcomes):
                o = outcomes[i]
                return o.get("name") if isinstance(o, dict) else str(o)
        except (ValueError, TypeError):
            pass
        return str(idx)

    winner = _outcome_name(winning_outcome)
    verdict = ""
    if held_outcome is not None and winning_outcome is not None:
        if int(held_outcome) == int(winning_outcome):
            verdict = f"\n{t(chat_id, 'resolve_win')}"
        else:
            verdict = f"\n{t(chat_id, 'resolve_lose')}"
    return (
        f"🏁 <b>{_title_html(title[:55], market)}</b>\n"
        f"{t(chat_id, 'label_resolve_outcome')}: <b>{_html_escape(winner)}</b>"
        f"{verdict}"
    )


def _sort_positions(positions: list[dict], sort_key: str) -> list[dict]:
    def _value(p):
        _, _, shares, price = display_fields(p, p.get("_market"))
        try:
            return float(shares) * float(price) / 100
        except (ValueError, TypeError):
            return 0.0

    def _size(p):
        s = pos_size(p)
        return s if s is not None else 0.0

    def _title(p):
        title, _, _, _ = display_fields(p, p.get("_market"))
        return (title or "").lower()

    if sort_key == "size":
        return sorted(positions, key=_size, reverse=True)
    if sort_key == "alpha":
        return sorted(positions, key=_title)
    # Default: sort by notional value descending.
    return sorted(positions, key=_value, reverse=True)


def fmt_summary(
    positions: list[dict],
    chat_id: int,
    sort_key: str = "value",
    include_portfolio: bool = False,
) -> str:
    if not positions:
        return t(chat_id, "fmt_no_positions")

    ordered = _sort_positions(positions, sort_key)

    total = 0.0
    total_pnl = 0.0
    total_pnl_known = False
    largest_val = 0.0
    largest_title = ""
    lines = []

    for p in ordered[:20]:
        title, outcome, shares, price = display_fields(p, p.get("_market"))
        title = title[:40]
        title_html = _title_html(title, p.get("_market"))
        pnl_usd, pnl_pct, _ = pnl_of(p, p.get("_market"))

        try:
            # price is in cents, convert to USD.
            v = float(shares) * float(price) / 100
            total += v
            if v > largest_val:
                largest_val = v
                largest_title = title
            pnl_tail = ""
            if pnl_usd is not None and pnl_pct is not None:
                sign = "+" if pnl_usd >= 0 else "−"
                arrow = "🟢" if pnl_usd > 0 else ("🔴" if pnl_usd < 0 else "⚪")
                pnl_tail = f"  {arrow} {sign}${abs(pnl_usd):,.2f} ({sign}{abs(pnl_pct):.2f}%)"
                total_pnl += pnl_usd
                total_pnl_known = True
            lines.append(f"• {outcome} | {shares} @ {price}c = ${v:,.2f}{pnl_tail}\n  {title_html}")
        except (ValueError, TypeError):
            lines.append(f"• {outcome} | {shares} @ {price}c\n  {title_html}")

    header = t(chat_id, "fmt_positions_header", count=len(positions), total=total)
    if total_pnl_known:
        sign = "+" if total_pnl >= 0 else "−"
        arrow = "🟢" if total_pnl > 0 else ("🔴" if total_pnl < 0 else "⚪")
        header += f"{t(chat_id, 'label_total_pnl')}: {arrow} <b>{sign}${abs(total_pnl):,.2f}</b>\n\n"

    if include_portfolio and largest_title:
        header += (
            f"{t(chat_id, 'label_largest_pos')}: <b>{_html_escape(largest_title)}</b> "
            f"(${largest_val:,.2f})\n\n"
        )

    sort_label = t(chat_id, f"sort_{sort_key}")
    header += f"<i>{t(chat_id, 'label_sorted_by')}: {sort_label}</i>\n\n"

    return header + "\n\n".join(lines)


def match_key(match: dict) -> str:
    tx = match.get("transactionHash", "")
    executed_at = match.get("executedAt", "")
    amount = str(match.get("amountFilled", ""))
    return f"{tx}:{executed_at}:{amount}"


def _match_price_and_usd(match: dict) -> tuple[float | None, float | None]:
    """Return (price_cents, total_usd) for a match fill.

    Prefers on-chain filled amounts (maker/taker) over the API's
    `priceExecuted`, which can arrive in varying scales. Falls back to the
    API-reported price only when on-chain amounts are missing.
    """
    shares = _norm_amount_to_shares(match.get("amountFilled"))
    maker_amt = _norm_amount_to_shares(match.get("makerAmountFilled"))
    taker_amt = _norm_amount_to_shares(match.get("takerAmountFilled"))

    price_c = None
    total_usd = None
    if (
        shares is not None and shares > 0
        and maker_amt is not None and maker_amt > 0
        and taker_amt is not None and taker_amt > 0
    ):
        # One side is the position token (≈ `amountFilled`), the other is the
        # collateral. Pick whichever is closer to `shares` as the position
        # side; the other is the collateral total.
        if abs(maker_amt - shares) <= abs(taker_amt - shares):
            total_usd = taker_amt
        else:
            total_usd = maker_amt
        price_c = total_usd / shares * 100

    if price_c is None:
        taker = match.get("taker") if isinstance(match.get("taker"), dict) else {}
        price_c = _norm_price_to_cents(match.get("priceExecuted") or taker.get("price"))

    if total_usd is None and shares is not None and price_c is not None:
        total_usd = shares * price_c / 100

    return price_c, total_usd


def _parse_executed_at(raw) -> float | None:
    """Parse a match's executedAt field to epoch seconds.

    Predict.fun has historically emitted ISO-8601 strings and epoch numbers
    (seconds or milliseconds); accept all three.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        v = float(raw)
        # Heuristic: values > 10^12 are ms.
        return v / 1000.0 if v > 1e12 else v
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        # Numeric string.
        try:
            v = float(s)
            return v / 1000.0 if v > 1e12 else v
        except ValueError:
            pass
        try:
            # Accept trailing Z for UTC.
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except ValueError:
            return None
    return None


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

    price_c, total_usd = _match_price_and_usd(match)

    if total_usd is not None:
        value_text = f"${total_usd:,.2f}"
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
            [InlineKeyboardButton(t(chat_id, "btn_help"), callback_data="help_root")],
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


SORT_CYCLE = ("value", "size", "alpha")


def _next_sort(current: str) -> str:
    try:
        return SORT_CYCLE[(SORT_CYCLE.index(current) + 1) % len(SORT_CYCLE)]
    except ValueError:
        return SORT_CYCLE[0]


ORDERS_PAGE_SIZE = 8
ORDERS_FETCH_LIMIT = 30


async def _render_positions(
    chat_id: int,
    addr: str,
    sort_key: str = "value",
    portfolio: bool = False,
) -> tuple[str, InlineKeyboardMarkup]:
    async with aiohttp.ClientSession() as session:
        positions, _ = await _fetch_positions_with_markets(session, addr)
    if positions is None:
        text = t(chat_id, "fetch_error_msg")
    else:
        text = (
            f"<code>{addr}</code>\n\n"
            f"{fmt_summary(positions, chat_id, sort_key=sort_key, include_portfolio=portfolio)}"
        )
    sort_label = t(chat_id, f"sort_{_next_sort(sort_key)}")
    portfolio_btn_key = "btn_hide_portfolio" if portfolio else "btn_portfolio"
    markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    t(chat_id, "btn_refresh"),
                    callback_data=f"refresh_pos:{addr}:{sort_key}:{1 if portfolio else 0}",
                ),
                InlineKeyboardButton(
                    t(chat_id, "btn_view_orders"), callback_data=f"orders:{addr}:0"
                ),
            ],
            [
                InlineKeyboardButton(
                    t(chat_id, "btn_sort", label=sort_label),
                    callback_data=f"sortpos:{addr}:{_next_sort(sort_key)}:{1 if portfolio else 0}",
                ),
                InlineKeyboardButton(
                    t(chat_id, portfolio_btn_key),
                    callback_data=f"sortpos:{addr}:{sort_key}:{0 if portfolio else 1}",
                ),
            ],
        ]
    )
    return text, markup


async def _render_orders(
    chat_id: int, addr: str, page: int = 0
) -> tuple[str, InlineKeyboardMarkup | None]:
    async with aiohttp.ClientSession() as session:
        matches = await fetch_order_matches(session, addr, first=ORDERS_FETCH_LIMIT)
        positions, positions_by_key = await _fetch_positions_with_markets(session, addr)

    if matches is None and positions is None:
        return t(chat_id, "fetch_error_msg"), None
    matches = matches or []
    if not matches:
        return f"<code>{addr}</code>\n\n{t(chat_id, 'no_orders')}", None

    matches = consolidate_fills(matches)
    total_pages = max(1, (len(matches) + ORDERS_PAGE_SIZE - 1) // ORDERS_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * ORDERS_PAGE_SIZE
    chunk = matches[start : start + ORDERS_PAGE_SIZE]

    lines = [t(chat_id, "orders_header", addr=fmt_addr(addr), count=len(matches)), ""]
    lines.extend(fmt_match(m, chat_id, positions_by_key) for m in chunk)

    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅", callback_data=f"orders:{addr}:{page - 1}"))
    nav.append(
        InlineKeyboardButton(
            t(chat_id, "list_page_indicator", page=page + 1, total=total_pages),
            callback_data="noop",
        )
    )
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("➡", callback_data=f"orders:{addr}:{page + 1}"))
    markup = InlineKeyboardMarkup([nav]) if total_pages > 1 else None
    return "\n\n".join(lines), markup


async def cmd_watch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if not ctx.args:
        # No args — user likely tapped /watch from the command menu. Show
        # the wallet-discovery guide plus a concrete 3-line format example
        # instead of a terse "Usage" line.
        await _send_watch_guide(ctx, chat_id)
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

    # Duplicate-note handling (T12): if the first address is already watched
    # and the user supplied a new note, ask before overwriting instead of
    # silently skipping.
    if (
        not multi_mode
        and base_note
        and _is_addr(addresses[0])
        and addresses[0] in watched.get(chat_id, {})
        and watched[chat_id][addresses[0]].note != base_note
    ):
        existing = watched[chat_id][addresses[0]]
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        t(chat_id, "btn_overwrite_note"),
                        callback_data=f"watchover:{addresses[0]}:{base_note[:50]}",
                    ),
                    InlineKeyboardButton(
                        t(chat_id, "btn_cancel"),
                        callback_data=f"watchover:{addresses[0]}:__CANCEL__",
                    ),
                ]
            ]
        )
        await update.message.reply_text(
            t(
                chat_id,
                "dup_watch_prompt",
                addr=fmt_addr(addresses[0]),
                old=_html_escape(existing.note or "(none)"),
                new=_html_escape(base_note),
            ),
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        return

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


STALE_THRESHOLD_S = 14 * 24 * 3600  # 14 days with no activity → 💤
FRESH_THRESHOLD_S = 60  # watch added less than 60s ago → 🌱


def _wallet_badge(chat_id: int, w: WatchedWallet) -> str:
    now = time.time()
    if w.muted:
        return t(chat_id, "label_muted_flag")
    # Fresh badge: brand-new wallet with no positions yet and checked just now.
    if (
        not w.position_snapshot
        and w.last_check
        and (now - w.last_check) < FRESH_THRESHOLD_S
        and not w.last_activity
    ):
        return t(chat_id, "label_fresh_flag")
    # Dormant badge: no activity for a long time and no positions.
    if (
        not w.position_snapshot
        and w.last_activity
        and (now - w.last_activity) > STALE_THRESHOLD_S
    ):
        return t(chat_id, "label_stale_flag")
    if (
        not w.position_snapshot
        and not w.last_activity
        and w.last_check
        and (now - w.last_check) > STALE_THRESHOLD_S
    ):
        return t(chat_id, "label_stale_flag")
    return t(chat_id, "label_watching_flag")


def _render_list_page(chat_id: int, page: int) -> tuple[str, InlineKeyboardMarkup | None]:
    wallets = list(watched.get(chat_id, {}).items())
    total_pages = max(1, (len(wallets) + LIST_PAGE_SIZE - 1) // LIST_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * LIST_PAGE_SIZE
    chunk = wallets[start : start + LIST_PAGE_SIZE]

    lines = [t(chat_id, "watch_list")]
    keyboard: list[list[InlineKeyboardButton]] = []
    for addr, w in chunk:
        flag = _wallet_badge(chat_id, w)
        note = f" · <b>{_html_escape(w.note)}</b>" if w.note else ""
        relt = _relative_time(chat_id, w.last_check)
        lines.append(
            f"{flag} <code>{addr}</code>{note}\n"
            f"    {len(w.position_snapshot)} pos · {t(chat_id, 'label_last_check')}: {relt}"
        )
        label_title = (w.note or fmt_addr(addr))[:18]
        mute_label = t(chat_id, "btn_unmute") if w.muted else t(chat_id, "btn_mute")
        # Row 1: Positions · Fills · ✏️ (edit note)
        keyboard.append(
            [
                InlineKeyboardButton(
                    f"{t(chat_id, 'btn_view_positions')} {label_title}",
                    callback_data=f"pos:{addr}",
                ),
                InlineKeyboardButton(
                    t(chat_id, "btn_view_orders"), callback_data=f"orders:{addr}"
                ),
                InlineKeyboardButton(
                    t(chat_id, "btn_edit_note"), callback_data=f"editnote:{addr}"
                ),
            ]
        )
        # Row 2: mute toggle · unwatch
        keyboard.append(
            [
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
    text, markup = await _render_orders(chat_id, addr, page=0)
    try:
        await status.edit_text(
            text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=markup,
        )
    except BadRequest:
        await update.message.reply_text(
            text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=markup,
        )


def _aggregate_market_flow(
    chat_id_filter: int | None,
    window_s: int = MARKET_FLOW_WINDOW_S,
    limit: int = 10,
) -> list[tuple[str, float, int]]:
    """Return (market_id, total_usd, trade_count) sorted by volume desc.

    Prunes entries older than ``window_s``. When ``chat_id_filter`` is set,
    only counts fills contributed by that chat.
    """
    cutoff = time.time() - window_s
    results: list[tuple[str, float, int]] = []
    for mid, buf in list(market_flow.items()):
        # Drop stale entries from the left in-place.
        while buf and buf[0][0] < cutoff:
            buf.popleft()
        if not buf:
            continue
        if chat_id_filter is None:
            total = sum(usd for _, usd, _ in buf)
            count = len(buf)
        else:
            total = 0.0
            count = 0
            for ts, usd, cid in buf:
                if cid == chat_id_filter:
                    total += usd
                    count += 1
            if count == 0:
                continue
        results.append((mid, total, count))
    results.sort(key=lambda x: x[1], reverse=True)
    return results[:limit]


async def cmd_hot(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    scope_mine = bool(ctx.args) and ctx.args[0].lower() in ("mine", "me", "my")
    ranking = _aggregate_market_flow(
        chat_id_filter=chat_id if scope_mine else None,
    )
    if not ranking:
        await update.message.reply_text(t(chat_id, "hot_empty"))
        return

    scope_label = t(chat_id, "hot_scope_mine" if scope_mine else "hot_scope_all")
    lines: list[str] = [t(chat_id, "hot_header", scope=scope_label)]

    async with aiohttp.ClientSession() as session:
        for rank, (mid, total_usd, count) in enumerate(ranking, start=1):
            market = market_cache.get(mid) or await fetch_market(session, mid)
            title = (
                (market or {}).get("title")
                or (market or {}).get("question")
                or market_flow_titles.get(mid)
                or mid
            )
            title = title[:55]
            lines.append(
                t(
                    chat_id,
                    "hot_line",
                    rank=rank,
                    title=_title_html(title, market if market else None),
                    usd=total_usd,
                    count=count,
                )
            )

    await update.message.reply_text(
        "\n\n".join(lines),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


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


def _settings_body(chat_id: int) -> str:
    wallets = watched.get(chat_id, {})
    muted_count = sum(1 for w in wallets.values() if w.muted)
    lang_label = "🇨🇳 中文" if get_lang(chat_id) == "zh" else "🇺🇸 English"
    mode = chat_notify.get(chat_id, "split")
    mode_label = t(chat_id, f"notify_{mode}")
    return "\n".join(
        [
            t(chat_id, "settings_title"),
            "",
            t(chat_id, "settings_lang", lang=lang_label),
            t(chat_id, "settings_interval", interval=POLL_INTERVAL),
            t(chat_id, "settings_watches", count=len(wallets), muted=muted_count),
            t(chat_id, "settings_notify", mode=mode_label),
        ]
    )


def _settings_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    mode = chat_notify.get(chat_id, "split")
    # Show the opposite of the current mode as the toggle target.
    target = "merged" if mode == "split" else "split"
    target_label_key = "btn_notify_merged" if target == "merged" else "btn_notify_split"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🇺🇸 English", callback_data="lang_en"),
                InlineKeyboardButton("🇨🇳 中文", callback_data="lang_zh"),
            ],
            [
                InlineKeyboardButton(
                    t(chat_id, target_label_key), callback_data=f"notify:{target}"
                )
            ],
        ]
    )


async def cmd_settings(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(
        _settings_body(chat_id),
        parse_mode="HTML",
        reply_markup=_settings_keyboard(chat_id),
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


async def cmd_threshold(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if len(ctx.args) < 2:
        await update.message.reply_text(t(chat_id, "usage_threshold"))
        return
    addr = resolve_addr(chat_id, ctx.args[0])
    target = watched.get(chat_id, {}).get(addr) if addr else None
    if not target:
        await update.message.reply_text(t(chat_id, "not_found"))
        return
    try:
        pct = float(ctx.args[1])
    except ValueError:
        await update.message.reply_text(t(chat_id, "usage_threshold"))
        return
    pct = max(0.0, min(100.0, pct))
    target.change_threshold_pct = pct
    save_watch(target)
    await update.message.reply_text(
        t(chat_id, "threshold_set", addr=fmt_addr(addr), pct=f"{pct:g}"),
        parse_mode="HTML",
    )


async def cmd_interval(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if len(ctx.args) < 2:
        await update.message.reply_text(t(chat_id, "usage_interval"))
        return
    addr = resolve_addr(chat_id, ctx.args[0])
    target = watched.get(chat_id, {}).get(addr) if addr else None
    if not target:
        await update.message.reply_text(t(chat_id, "not_found"))
        return
    try:
        sec = int(ctx.args[1])
    except ValueError:
        await update.message.reply_text(t(chat_id, "usage_interval"))
        return
    if sec != 0 and sec < 5:
        await update.message.reply_text(t(chat_id, "interval_too_small"))
        return
    target.poll_interval_s = sec
    save_watch(target)
    await update.message.reply_text(
        t(chat_id, "interval_set", addr=fmt_addr(addr), sec=sec),
        parse_mode="HTML",
    )


def _find_outcome_index(
    chat_id: int, addr: str, outcome_name: str
) -> tuple[str | None, int | None]:
    """Search the wallet's cached titles / positions for an outcome match.

    Returns (market_id, outcome_index). We scan the live poll snapshot so the
    user can reference outcomes by name (e.g. "YES"/"NO"/"Trump") rather than
    having to know the internal market ID. Returns (None, None) when nothing
    matches.
    """
    w = watched.get(chat_id, {}).get(addr)
    if not w:
        return None, None
    want = outcome_name.lower()
    # position_titles keys are pos_key == "<market_id>_<idx>". We don't have
    # the outcome name in there, so fall back to looking at the live market
    # cache for any outcome whose name matches.
    for k in w.position_snapshot.keys():
        market_id, _, idx_str = k.rpartition("_")
        if not market_id:
            continue
        market = market_cache.get(market_id) or {}
        outcomes = market.get("outcomes") or market.get("outcomeNames") or []
        try:
            idx = int(idx_str)
        except ValueError:
            continue
        if isinstance(outcomes, list) and 0 <= idx < len(outcomes):
            o = outcomes[idx]
            name = (o.get("name") if isinstance(o, dict) else str(o)) or ""
            if name.lower() == want:
                return market_id, idx
    # Second pass: any outcome in market_cache for any of the user's markets.
    for k in w.position_snapshot.keys():
        market_id, _, _ = k.rpartition("_")
        market = market_cache.get(market_id) or {}
        outcomes = market.get("outcomes") or market.get("outcomeNames") or []
        if isinstance(outcomes, list):
            for i, o in enumerate(outcomes):
                name = (o.get("name") if isinstance(o, dict) else str(o)) or ""
                if name.lower() == want:
                    return market_id, i
    return None, None


async def cmd_alert(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if len(ctx.args) < 4:
        await update.message.reply_text(t(chat_id, "usage_alert"))
        return
    addr = resolve_addr(chat_id, ctx.args[0])
    target = watched.get(chat_id, {}).get(addr) if addr else None
    if not target:
        await update.message.reply_text(t(chat_id, "not_found"))
        return
    outcome_name = ctx.args[1]
    op = ctx.args[2]
    if op not in (">=", "<="):
        await update.message.reply_text(t(chat_id, "alert_bad_op"))
        return
    try:
        threshold = float(ctx.args[3])
    except ValueError:
        await update.message.reply_text(t(chat_id, "alert_bad_price"))
        return
    if not (0 <= threshold <= 100):
        await update.message.reply_text(t(chat_id, "alert_bad_price"))
        return

    market_id, outcome_index = _find_outcome_index(chat_id, addr, outcome_name)
    if not market_id or outcome_index is None:
        await update.message.reply_text(t(chat_id, "alert_bad_outcome"))
        return

    alert_id = insert_alert(chat_id, addr, market_id, outcome_index, op, threshold)
    await update.message.reply_text(
        t(
            chat_id,
            "alert_created",
            id=alert_id,
            outcome=_html_escape(outcome_name),
            op=op,
            threshold=threshold,
        ),
        parse_mode="HTML",
    )


async def cmd_alerts(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    addr = None
    if ctx.args:
        addr = resolve_addr(chat_id, ctx.args[0])
    alerts = list_alerts(chat_id, addr)
    if not alerts:
        await update.message.reply_text(t(chat_id, "alerts_empty"))
        return
    lines = [t(chat_id, "alerts_header", count=len(alerts))]
    for a in alerts:
        market = market_cache.get(a["market_id"]) or {}
        outcomes = market.get("outcomes") or market.get("outcomeNames") or []
        outcome_name = "?"
        try:
            idx = int(a["outcome_index"])
            if isinstance(outcomes, list) and 0 <= idx < len(outcomes):
                o = outcomes[idx]
                outcome_name = o.get("name") if isinstance(o, dict) else str(o)
        except (ValueError, TypeError):
            pass
        title = (market.get("title") or market.get("question") or a["market_id"])[:45]
        lines.append(
            t(
                chat_id,
                "alerts_line",
                id=a["id"],
                outcome=_html_escape(str(outcome_name)),
                op=a["op"],
                threshold=a["threshold"],
                title=_html_escape(title),
            )
        )
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_unalert(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not ctx.args:
        await update.message.reply_text(t(chat_id, "usage_unalert"))
        return
    try:
        alert_id = int(ctx.args[0])
    except ValueError:
        await update.message.reply_text(t(chat_id, "usage_unalert"))
        return
    ok = delete_alert(chat_id, alert_id)
    key = "unalert_ok" if ok else "unalert_not_found"
    await update.message.reply_text(
        t(chat_id, key, id=alert_id),
        parse_mode="HTML",
    )


async def cmd_portfolio(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Reuse /pos rendering but open with the portfolio card expanded."""
    chat_id = update.effective_chat.id
    if not ctx.args:
        await update.message.reply_text(t(chat_id, "usage_pos"))
        return
    addr = resolve_addr(chat_id, ctx.args[0])
    if not addr:
        await update.message.reply_text(t(chat_id, "invalid_address"))
        return
    status = await update.message.reply_text(t(chat_id, "loading_positions"))
    text, markup = await _render_positions(
        chat_id, addr, sort_key="value", portfolio=True
    )
    try:
        await status.edit_text(
            text, parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True
        )
    except BadRequest:
        await update.message.reply_text(
            text, parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True
        )


# ---- /help browser ----------------------------------------------------


def _help_root_markup(chat_id: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for cat_id, _ in HELP_CATEGORIES:
        rows.append(
            [
                InlineKeyboardButton(
                    t(chat_id, f"help_cat_{cat_id}"),
                    callback_data=f"help_cat:{cat_id}",
                )
            ]
        )
    return InlineKeyboardMarkup(rows)


def _help_category_markup(chat_id: int, cat_id: str) -> InlineKeyboardMarkup:
    cmds = next((cs for cid, cs in HELP_CATEGORIES if cid == cat_id), [])
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for c in cmds:
        row.append(
            InlineKeyboardButton(f"/{c}", callback_data=f"help_cmd:{c}:{cat_id}")
        )
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(t(chat_id, "btn_back"), callback_data="help_root")])
    return InlineKeyboardMarkup(rows)


def _help_command_markup(chat_id: int, cat_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(t(chat_id, "btn_back"), callback_data=f"help_cat:{cat_id}")]]
    )


def _help_category_text(chat_id: int, cat_id: str) -> str:
    cmds = next((cs for cid, cs in HELP_CATEGORIES if cid == cat_id), [])
    # Tiny one-liner per command — first line of each help blurb.
    lines = [t(chat_id, f"help_cat_header_{cat_id}")]
    for c in cmds:
        blurb = t(chat_id, f"help_cmd_{c}")
        first = blurb.split("\n", 1)[0]
        lines.append(f"• {first}")
    lines.append("")
    lines.append("👇")
    return "\n".join(lines)


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(
        t(chat_id, "help_title"),
        parse_mode="HTML",
        reply_markup=_help_root_markup(chat_id),
        disable_web_page_preview=True,
    )


# ---- /import: either reply-to a JSON document OR forward a JSON document.

MAX_IMPORT_BYTES = 256 * 1024


async def cmd_import(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    msg = update.message
    target_msg = msg.reply_to_message if msg and msg.reply_to_message else msg
    doc = getattr(target_msg, "document", None) if target_msg else None
    if not doc:
        await msg.reply_text(t(chat_id, "import_no_file"))
        return
    if doc.file_size and doc.file_size > MAX_IMPORT_BYTES:
        await msg.reply_text(t(chat_id, "import_too_big"))
        return
    try:
        tg_file = await doc.get_file()
        buf = BytesIO()
        await tg_file.download_to_memory(out=buf)
        buf.seek(0)
        payload = json.loads(buf.read().decode("utf-8"))
    except Exception as e:
        logger.warning(f"/import parse failed: {e}")
        await msg.reply_text(t(chat_id, "import_parse_error"))
        return

    records = payload.get("watches") if isinstance(payload, dict) else payload
    if not isinstance(records, list):
        await msg.reply_text(t(chat_id, "import_parse_error"))
        return

    added = 0
    skipped = 0
    watched.setdefault(chat_id, {})
    async with aiohttp.ClientSession() as session:
        for rec in records:
            if not isinstance(rec, dict):
                skipped += 1
                continue
            addr = str(rec.get("address") or "").strip()
            if not (addr.startswith("0x") and len(addr) == 42):
                skipped += 1
                continue
            if addr.lower() in {a.lower() for a in watched[chat_id]}:
                skipped += 1
                continue

            positions = await fetch_positions(session, addr)
            matches = await fetch_order_matches(session, addr, first=30)
            if positions is None and matches is None:
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
                note=str(rec.get("note") or ""),
                muted=bool(rec.get("muted", False)),
                position_snapshot=snapshot,
                position_titles=title_cache,
                order_match_snapshot={match_key(m) for m in matches},
                last_check=time.time(),
            )
            save_watch(watched[chat_id][addr])
            added += 1

    await msg.reply_text(
        t(chat_id, "import_done", added=added, skipped=skipped), parse_mode="HTML"
    )


# ---- Inline note editing (reply-based) ----

# Maps chat_id to address awaiting a note reply. Tracked in user_data, but we
# also keep a module-level fallback for group chats where user_data is keyed
# differently.
def _set_pending_note_edit(ctx: ContextTypes.DEFAULT_TYPE, chat_id: int, addr: str):
    ctx.user_data["pending_note_edit"] = {"chat_id": chat_id, "address": addr}


def _pop_pending_note_edit(ctx: ContextTypes.DEFAULT_TYPE) -> dict | None:
    return ctx.user_data.pop("pending_note_edit", None)


async def on_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle free-text messages. Currently only the note-edit reply flow."""
    if not update.message or update.message.text is None:
        return
    chat_id = update.effective_chat.id
    pending = _pop_pending_note_edit(ctx)
    if not pending or pending.get("chat_id") != chat_id:
        return
    addr = pending.get("address") or ""
    target = watched.get(chat_id, {}).get(addr)
    if not target:
        return
    target.note = update.message.text.strip()
    save_watch(target)
    await update.message.reply_text(
        t(chat_id, "edit_note_saved", addr=fmt_addr(addr), note=_html_escape(target.note)),
        parse_mode="HTML",
    )


# ==================== Callback handling ====================


async def _show_positions_via_callback(
    query,
    ctx,
    chat_id: int,
    addr: str,
    edit: bool,
    sort_key: str = "value",
    portfolio: bool = False,
):
    text, markup = await _render_positions(
        chat_id, addr, sort_key=sort_key, portfolio=portfolio
    )
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

    # notify:<split|merged> — toggle the per-chat notification layout.
    if data.startswith("notify:"):
        mode = data.split(":", 1)[1].strip()
        if mode not in NOTIFY_MODES:
            return
        chat_notify[chat_id] = mode
        save_chat_notify(chat_id, mode)
        try:
            await query.edit_message_text(
                _settings_body(chat_id),
                parse_mode="HTML",
                reply_markup=_settings_keyboard(chat_id),
            )
        except BadRequest:
            pass
        return

    # pos:<addr> | refresh_pos:<addr>[:sort[:portfolio]] | sortpos:<addr>:<sort>:<portfolio>
    if data.startswith("pos:") or data.startswith("refresh_pos:") or data.startswith("sortpos:"):
        parts = data.split(":")
        addr = parts[1].strip() if len(parts) > 1 else ""
        sort_key = parts[2] if len(parts) > 2 else "value"
        portfolio = bool(int(parts[3])) if len(parts) > 3 and parts[3].isdigit() else False
        if sort_key not in SORT_CYCLE:
            sort_key = "value"
        if not addr.startswith("0x") or len(addr) != 42:
            await ctx.bot.send_message(chat_id=chat_id, text=t(chat_id, "invalid_address"))
            return
        edit = data.startswith("refresh_pos:") or data.startswith("sortpos:")
        await _show_positions_via_callback(
            query, ctx, chat_id, addr, edit=edit, sort_key=sort_key, portfolio=portfolio
        )
        return

    # orders:<addr>[:page]
    if data.startswith("orders:"):
        parts = data.split(":")
        addr = parts[1].strip() if len(parts) > 1 else ""
        try:
            page = int(parts[2]) if len(parts) > 2 else 0
        except ValueError:
            page = 0
        if not addr.startswith("0x") or len(addr) != 42:
            await ctx.bot.send_message(chat_id=chat_id, text=t(chat_id, "invalid_address"))
            return
        text, markup = await _render_orders(chat_id, addr, page=page)
        # B2: edit the existing message if the button came from an existing
        # orders view (page > 0 or tapping again). Fall back to sending new.
        try:
            await query.edit_message_text(
                text,
                parse_mode="HTML",
                reply_markup=markup,
                disable_web_page_preview=True,
            )
        except BadRequest:
            await ctx.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="HTML",
                reply_markup=markup,
                disable_web_page_preview=True,
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

    # editnote:<addr> — prompt the user to reply with the new note.
    if data.startswith("editnote:"):
        addr = data.split(":", 1)[1].strip()
        if addr not in watched.get(chat_id, {}):
            return
        _set_pending_note_edit(ctx, chat_id, addr)
        await ctx.bot.send_message(
            chat_id=chat_id,
            text=t(chat_id, "edit_note_prompt", addr=fmt_addr(addr)),
            parse_mode="HTML",
            reply_markup=ForceReply(selective=True),
        )
        return

    # watchover:<addr>:<base64-note> — confirm overwriting an existing note.
    if data.startswith("watchover:"):
        parts = data.split(":", 2)
        addr = parts[1].strip() if len(parts) > 1 else ""
        new_note = parts[2] if len(parts) > 2 else ""
        target = watched.get(chat_id, {}).get(addr)
        if target and new_note != "__CANCEL__":
            target.note = new_note
            save_watch(target)
            try:
                await query.edit_message_text(t(chat_id, "dup_watch_overwritten"))
            except BadRequest:
                pass
        else:
            try:
                await query.edit_message_text(t(chat_id, "dup_watch_cancelled"))
            except BadRequest:
                pass
        return

    if data == "watch_guide":
        await _send_watch_guide(ctx, chat_id)
        return

    # /help browser callbacks.
    if data == "help_root":
        try:
            await query.edit_message_text(
                t(chat_id, "help_title"),
                parse_mode="HTML",
                reply_markup=_help_root_markup(chat_id),
                disable_web_page_preview=True,
            )
        except BadRequest:
            pass
        return

    if data.startswith("help_cat:"):
        cat_id = data.split(":", 1)[1]
        if cat_id not in {cid for cid, _ in HELP_CATEGORIES}:
            return
        try:
            await query.edit_message_text(
                _help_category_text(chat_id, cat_id),
                parse_mode="HTML",
                reply_markup=_help_category_markup(chat_id, cat_id),
                disable_web_page_preview=True,
            )
        except BadRequest:
            pass
        return

    if data.startswith("help_cmd:"):
        parts = data.split(":")
        cmd = parts[1] if len(parts) > 1 else ""
        back_cat = parts[2] if len(parts) > 2 else "other"
        i18n_key = f"help_cmd_{cmd}"
        # Safety net: if someone passes an unknown command, bounce back.
        if i18n_key not in I18N.get(get_lang(chat_id), {}) and i18n_key not in I18N["en"]:
            return
        try:
            await query.edit_message_text(
                t(chat_id, i18n_key),
                parse_mode="HTML",
                reply_markup=_help_command_markup(chat_id, back_cat),
                disable_web_page_preview=True,
            )
        except BadRequest:
            pass
        return


async def _send_watch_guide(ctx: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Send the wallet-discovery guide (image + caption, or text fallback)."""
    guide_path = Path(GUIDE_IMAGE_PATH)
    example = t(chat_id, "watch_example")
    if guide_path.exists():
        with open(guide_path, "rb") as photo:
            await ctx.bot.send_photo(
                chat_id=chat_id,
                photo=photo,
                caption=t(chat_id, "watch_guide_caption") + "\n\n" + example,
                parse_mode="HTML",
            )
    else:
        await ctx.bot.send_message(
            chat_id=chat_id,
            text=t(chat_id, "watch_guide_text") + "\n\n" + example,
            parse_mode="HTML",
        )

# ==================== Poll Loop ====================

async def _evaluate_alerts(
    app: Application,
    chat_id: int,
    w: WatchedWallet,
    positions_by_key: dict,
    markets: dict,
):
    """Check each stored price alert for this wallet; fire and delete matches."""
    try:
        alerts = list_alerts(chat_id, w.address)
    except Exception as e:
        logger.warning(f"list_alerts failed for {w.address}: {e}")
        return
    if not alerts:
        return

    for a in alerts:
        mid = a["market_id"]
        oi = a["outcome_index"]
        lookup = _position_lookup_key(mid, oi)
        pos = positions_by_key.get(lookup)
        if pos is None:
            # Position no longer held — can't evaluate against mark price.
            continue
        mark = mark_price_cents(pos, markets.get(mid) or pos.get("_market"))
        if mark is None:
            continue
        op = a["op"]
        thr = float(a["threshold"])
        hit = (op == ">=" and mark >= thr) or (op == "<=" and mark <= thr)
        if not hit:
            continue
        title, outcome, _, _ = display_fields(pos, pos.get("_market"))
        try:
            if not w.muted:
                await app.bot.send_message(
                    chat_id=chat_id,
                    text=t(
                        chat_id,
                        "alert_fired",
                        addr=fmt_addr(w.address),
                        title=_title_html(title[:55], pos.get("_market")),
                        outcome=_html_escape(outcome),
                        op=op,
                        threshold=thr,
                        price=mark,
                    ),
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
            delete_alert(chat_id, a["id"])
        except Exception as e:
            logger.warning(f"alert send failed for {w.address}: {e}")


def _detect_resolutions(
    w: WatchedWallet,
    markets: dict,
    old_titles: dict,
    positions_by_key: dict,
) -> list[tuple[dict, int | None, int | None, str]]:
    """Scan watched markets for a resolved transition we haven't alerted on yet.

    Returns a list of ``(market, winning_idx, held_idx, title)`` for markets
    that have newly resolved. Mutates ``w.resolved_markets`` to record the
    new state so we fire only once per resolution.
    """
    newly: list[tuple[dict, int | None, int | None, str]] = []
    for mid, market in markets.items():
        resolved, winning = market_resolution(market)
        if not resolved:
            continue
        prior = w.resolved_markets.get(str(mid))
        if prior:
            continue
        # Try to find which outcome (if any) this wallet held.
        held_idx: int | None = None
        for k, p in positions_by_key.items():
            if str(p.get("marketId") or (p.get("market") or {}).get("id")) == str(mid):
                oi = p.get("outcomeIndex")
                if oi is None:
                    oi = (p.get("outcome") or {}).get("index")
                try:
                    held_idx = int(oi)
                except (ValueError, TypeError):
                    held_idx = None
                break
        title = market.get("title") or market.get("question") or str(mid)
        # Also check old_titles (in case the position just closed this cycle).
        if held_idx is None:
            for k, title_cache in old_titles.items():
                if k.startswith(f"{mid}_"):
                    try:
                        held_idx = int(k.rsplit("_", 1)[-1])
                    except ValueError:
                        pass
                    break
        w.resolved_markets[str(mid)] = {"winning": winning, "ts": time.time()}
        newly.append((market, winning, held_idx, title))
    return newly


async def poll_loop(app: Application):
    await asyncio.sleep(3)
    logger.info("Poll loop started")

    async with aiohttp.ClientSession() as session:
        while True:
            for chat_id, wallets in list(watched.items()):
                for addr, w in list(wallets.items()):
                    try:
                        # Per-wallet interval gating: skip wallets whose
                        # personal interval hasn't elapsed. 0 = use global.
                        if (
                            w.poll_interval_s
                            and (time.time() - w.last_check) < w.poll_interval_s
                        ):
                            continue

                        positions_raw = await fetch_positions(session, w.address)
                        matches_raw = await fetch_order_matches(session, w.address, first=20)

                        # B1 fix: count as failure if EITHER call failed.
                        # Reset only when BOTH succeed.
                        any_failed = positions_raw is None or matches_raw is None
                        all_failed = positions_raw is None and matches_raw is None

                        if any_failed:
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
                            if all_failed:
                                # No data at all — can't meaningfully diff.
                                await asyncio.sleep(1)
                                continue

                        # Both succeeded → recover state and notify if needed.
                        if not any_failed:
                            was_notified = w.error_notified
                            w.fetch_errors = 0
                            w.error_notified = False
                            if was_notified and not w.muted:
                                try:
                                    await app.bot.send_message(
                                        chat_id=chat_id,
                                        text=t(
                                            chat_id,
                                            "api_recovered",
                                            addr=fmt_addr(w.address),
                                        ),
                                        parse_mode="HTML",
                                    )
                                except Exception as send_err:
                                    logger.warning(
                                        f"Failed to send api_recovered for {addr}: {send_err}"
                                    )

                        # Only diff / refresh snapshots for endpoints that
                        # actually returned data. A transient 5xx on one call
                        # used to be coerced to [], which flagged every
                        # holding as "closed" and then re-announced them all
                        # as "new" on the next successful poll.
                        positions_ok = positions_raw is not None
                        matches_ok = matches_raw is not None
                        positions = positions_raw or []
                        matches = matches_raw or []
                        market_ids = {p.get("marketId") for p in positions if p.get("marketId")}
                        markets = {mid: await fetch_market(session, mid) for mid in market_ids}
                        for p in positions:
                            p["_market"] = markets.get(p.get("marketId"), {})
                        positions_by_key = {pos_key(p): p for p in positions}

                        if positions_ok:
                            added, changed, closed = diff_positions(
                                w.position_snapshot,
                                positions,
                                threshold_pct=w.change_threshold_pct,
                            )
                        else:
                            added, changed, closed = [], [], []

                        if matches_ok:
                            new_match_keys = {match_key(m) for m in matches}
                            new_fills = [
                                m for m in matches
                                if match_key(m) not in w.order_match_snapshot
                            ]
                        else:
                            new_match_keys = None
                            new_fills = []

                        # Feed the /hot rolling volume buffer with anything we
                        # haven't seen before. `new_fills` is already
                        # deduplicated against the per-wallet snapshot.
                        for fill in new_fills:
                            fmkt = fill.get("market") if isinstance(fill.get("market"), dict) else {}
                            mid = fmkt.get("id")
                            if not mid:
                                continue
                            ts = _parse_executed_at(fill.get("executedAt"))
                            if ts is None:
                                ts = time.time()
                            _, usd = _match_price_and_usd(fill)
                            if usd is None or usd <= 0:
                                continue
                            market_flow[str(mid)].append((ts, float(usd), chat_id))
                            ftitle = fmkt.get("title") or fmkt.get("question")
                            if ftitle:
                                market_flow_titles[str(mid)] = ftitle[:80]

                        # Refresh the shares snapshot + the title cache. Hold onto
                        # old titles so "closed" notifications can show a real
                        # market name even though the position is gone now.
                        new_titles: dict[str, str] = {}
                        for p in positions:
                            title, _, _, _ = display_fields(p, p.get("_market"))
                            new_titles[pos_key(p)] = title[:80]
                        old_titles = dict(w.position_titles)
                        # Preserve titles for keys that have disappeared this
                        # poll so we can still render their resolution notice.
                        merged_titles = {**old_titles, **new_titles}
                        if positions_ok:
                            w.position_titles = new_titles
                            w.position_snapshot = {pos_key(p): pos_size(p) for p in positions}
                        w.last_check = time.time()
                        display_addr = fmt_addr(w.address) + (f" · {w.note}" if w.note else "")

                        # --- Detect market resolutions (fires once per market) ---
                        resolutions = _detect_resolutions(
                            w, markets, merged_titles, positions_by_key
                        )

                        blocks: list[str] = []  # For merged combined message.

                        if added or changed or closed:
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
                                title = merged_titles.get(k) or t(
                                    chat_id, "close_fallback", key=k
                                )
                                parts.append(
                                    f"{t(chat_id, 'fmt_closed')} <b>{_html_escape(title)}</b>"
                                )
                            block = t(chat_id, "poll_header", addr=display_addr) + "\n\n".join(parts[:8])
                            if len(parts) > 8:
                                block += t(chat_id, "fmt_more", count=len(parts) - 8)
                            blocks.append(block)

                        if new_fills:
                            merged_fills = consolidate_fills(new_fills)
                            fill_lines = [
                                fmt_match(m, chat_id, positions_by_key)
                                for m in merged_fills[:5]
                            ]
                            blocks.append(
                                t(chat_id, "fills_header", addr=display_addr)
                                + "\n\n".join(fill_lines)
                            )

                        if resolutions:
                            lines = []
                            for market, winning_idx, held_idx, title in resolutions:
                                lines.append(
                                    fmt_resolution(chat_id, market, winning_idx, held_idx)
                                )
                            blocks.append(
                                t(chat_id, "poll_resolve_header", addr=display_addr)
                                + "\n\n".join(lines)
                            )

                        # Honor the chat's notification mode:
                        #   "split"  (default) — one Telegram message per
                        #            block (position changes / fills /
                        #            resolution).
                        #   "merged" — all blocks glued together with ───── so
                        #            one poll produces at most one message.
                        # In-block batching always applies: up to 5 fills per
                        # 订单成交 message, up to 8 position changes per 持仓变化
                        # message. This is about cross-block layout only.
                        if blocks and not w.muted:
                            mode = chat_notify.get(chat_id, "split")
                            if mode == "merged":
                                outgoing = ["\n\n─────\n\n".join(blocks)]
                            else:
                                outgoing = blocks
                            for msg in outgoing:
                                try:
                                    await app.bot.send_message(
                                        chat_id=chat_id,
                                        text=msg,
                                        parse_mode="HTML",
                                        disable_web_page_preview=True,
                                    )
                                    w.last_activity = time.time()
                                except Exception as send_err:
                                    logger.warning(
                                        f"Failed to send poll block for {addr}: {send_err}"
                                    )

                        # Price-alert evaluation runs regardless of mute state
                        # (alerts are explicit user-configured; mute only
                        # silences auto-detected change/fill spam).
                        await _evaluate_alerts(app, chat_id, w, positions_by_key, markets)

                        if new_match_keys is not None:
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
    # Trimmed to a daily-use core. Commands not in this list still work if
    # typed directly (mute/note/portfolio/lang/etc. are also exposed as inline
    # buttons in /list, /pos and /settings).
    await app.bot.set_my_commands(
        [
            BotCommand("start", "开始 / Start"),
            BotCommand("help", "功能介绍 / Help"),
            BotCommand("watch", "监控地址 / Watch wallet(s)"),
            BotCommand("unwatch", "取消监控 / Unwatch wallet"),
            BotCommand("list", "监控列表 / Watch list"),
            BotCommand("pos", "查询持仓 / View positions"),
            BotCommand("orders", "最近成交 / Recent fills"),
            BotCommand("hot", "1h 成交排行 / Top markets (1h)"),
            BotCommand("settings", "偏好设置 / Settings"),
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
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("watch", cmd_watch))
    app.add_handler(CommandHandler("unwatch", cmd_unwatch))
    app.add_handler(CommandHandler("list", cmd_list))
    app.add_handler(CommandHandler("pos", cmd_pos))
    app.add_handler(CommandHandler("positions", cmd_pos))
    app.add_handler(CommandHandler("orders", cmd_orders))
    app.add_handler(CommandHandler("hot", cmd_hot))
    app.add_handler(CommandHandler("note", cmd_note))
    app.add_handler(CommandHandler("mute", cmd_mute))
    app.add_handler(CommandHandler("unmute", cmd_unmute))
    app.add_handler(CommandHandler("settings", cmd_settings))
    app.add_handler(CommandHandler("export", cmd_export))
    app.add_handler(CommandHandler("import", cmd_import))
    app.add_handler(CommandHandler("threshold", cmd_threshold))
    app.add_handler(CommandHandler("interval", cmd_interval))
    app.add_handler(CommandHandler("alert", cmd_alert))
    app.add_handler(CommandHandler("alerts", cmd_alerts))
    app.add_handler(CommandHandler("unalert", cmd_unalert))
    app.add_handler(CommandHandler("portfolio", cmd_portfolio))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("lang", cmd_lang))
    app.add_handler(CallbackQueryHandler(on_callback))
    # Reply-based note edit capture (must not match /-commands).
    app.add_handler(
        MessageHandler(filters.REPLY & filters.TEXT & ~filters.COMMAND, on_message)
    )

    app.post_init = on_startup

    logger.info("Bot starting...")
    app.run_polling()


if __name__ == "__main__":
    main()
