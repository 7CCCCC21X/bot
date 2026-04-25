import os
import asyncio
import logging
import re
import time
import json
import csv
import sqlite3
import threading
from collections import deque
from io import BytesIO, StringIO
from pathlib import Path
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime

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
# Optional: admin chat id for /raw debug command. Leave unset to disable.
_raw_admin = os.environ.get("ADMIN_CHAT_ID", "").strip()
ADMIN_CHAT_ID = int(_raw_admin) if _raw_admin.lstrip("-").isdigit() else None
POLL_INTERVAL = 5
# Maximum number of wallets polled concurrently within a single cycle. The old
# loop processed wallets serially with a 1-second gap between each, so a chat
# watching N wallets saw a cycle of roughly N + POLL_INTERVAL seconds. Polling
# in parallel keeps end-to-end latency near POLL_INTERVAL regardless of N.
try:
    POLL_CONCURRENCY = max(1, int(os.environ.get("POLL_CONCURRENCY", "8") or 8))
except ValueError:
    POLL_CONCURRENCY = 8
TX_EXPLORER_BASE = os.environ.get("TX_EXPLORER_BASE", "https://bscscan.com/tx/")
# Fallback dust floor used when a wallet has no per-wallet `min_fill_usd` set.
# Set MIN_FILL_USD=0 to disable by default (every fill shows, even $0.04 dust).
# Sub-threshold fills are folded into a "💨 N 笔小额共 $X" summary line at the
# end of the 订单成交 block rather than dropped.
try:
    MIN_FILL_USD = float(os.environ.get("MIN_FILL_USD", "1") or 1)
except ValueError:
    MIN_FILL_USD = 1.0
# Fallback dust-summary flush interval (seconds). Defaults to 8 hours so
# micro-fill summaries batch instead of firing on every poll. Set to 0 to
# restore the legacy "flush every poll" behavior globally, or override per
# wallet via /dustinterval.
try:
    DUST_INTERVAL = int(os.environ.get("DUST_INTERVAL", "28800") or 28800)
except ValueError:
    DUST_INTERVAL = 28800
if DUST_INTERVAL < 0:
    DUST_INTERVAL = 0
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
    ("query", ["pos", "orders"]),
    ("other", ["settings", "defaults", "start"]),
]


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
    # Per-wallet minimum fill USD value. Fills below this notional fold into a
    # "💨 N 笔小额共 $X" summary line instead of firing individual cards. 0
    # falls back to the global MIN_FILL_USD env default.
    min_fill_usd: float = 0.0
    # Per-wallet dust-summary flush interval in seconds. 0 (default) = use
    # global DUST_INTERVAL. When the effective value is 0 the dust summary
    # fires every poll (legacy); when >0, dust fills accumulate in memory
    # and the 💨 summary is emitted once per interval.
    dust_interval_s: int = 0
    # Wall-clock of the last real activity (emitted notification). Powers the
    # "stale" badge in /list.
    last_activity: float = 0
    # Cache of market resolution state keyed by market_id so we only alert
    # once on resolution transitions.
    resolved_markets: dict = field(default_factory=dict)
    # Transient runtime counters (not persisted).
    fetch_errors: int = 0
    error_notified: bool = False
    # Rate-limit cooldown state (transient). `rate_limit_until` is a wall-clock
    # epoch second; while now < rate_limit_until, the polling loop skips this
    # wallet entirely. `rate_limit_level` counts consecutive 429s and drives
    # the exponential backoff fallback when no Retry-After header is present.
    rate_limit_until: float = 0
    rate_limit_level: int = 0
    rate_limit_notified: bool = False
    # Transient dust-summary accumulator. Holds match dicts for dust fills
    # that haven't been flushed yet (only used when the effective
    # dust_interval > 0). Not persisted — on restart the snapshot dedupe
    # logic keeps old fills out of the queue.
    pending_dust_fills: list = field(default_factory=list)
    last_dust_flush: float = 0


watched: dict[int, dict[str, WatchedWallet]] = {}
market_cache: dict[str, dict] = {}
chat_lang: dict[int, str] = {}
# Per-chat notification mode: "split" (default — one message per block) or
# "merged" (legacy T13 — position changes + fills + resolution joined by
# ───── into a single Telegram message).
chat_notify: dict[int, str] = {}
# Per-chat defaults for micro-fill settings. Keyed by chat_id; value has
# "min_fill_usd" (0 = inherit env MIN_FILL_USD) and "dust_interval_s"
# (0 = inherit env DUST_INTERVAL, -1 = every poll, >0 = that many seconds).
chat_defaults: dict[int, dict] = {}
# In-memory bulk-selection state for the watch list. Selecting checkboxes in
# /list lives here only — never persisted. Keyed by chat_id.
bulk_selection: dict[int, set[str]] = {}
# Rolling per-endpoint API status log. Each entry is (epoch_s, label) where
# label ∈ {"ok", "429", "5xx", "timeout", "other"}. Powers /apistatus. Capped
# at 2000 entries per endpoint (~roughly the last several hours of polling).
api_stats: dict[str, deque] = {
    "positions": deque(maxlen=2000),
    "matches": deque(maxlen=2000),
}
NOTIFY_MODES = ("split", "merged")
db_lock = threading.Lock()

I18N = {
    "en": {
        "choose_lang": "🌐 Choose language:",
        "btn_watch_wallet": "➕ Watch wallet",
        "btn_watch_guide": "📖 How to find wallet address",
        "watch_reply_prompt": (
            "📝 <b>Reply with a wallet address to watch</b>\n"
            "<i>Paste a single 0x… address, or one per line for a batch "
            "(optionally <code>&lt;addr&gt; note</code>). No /watch prefix.</i>"
        ),
        "watch_reply_invalid": (
            "❌ That's not a valid wallet address.\n\n"
            "Please reply with a valid Ethereum address starting with <code>0x</code> (42 characters)."
        ),
        "pos_reply_prompt": (
            "📊 <b>Reply with a wallet address to view its positions</b>\n"
            "<i>Paste a 0x… address or the note / alias of a watched wallet. No /pos prefix.</i>"
        ),
        "orders_reply_prompt": (
            "📜 <b>Reply with a wallet address to view recent fills</b>\n"
            "<i>Paste a 0x… address or the note / alias of a watched wallet. No /orders prefix.</i>"
        ),
        "portfolio_reply_prompt": (
            "📊 <b>Reply with a wallet address for the portfolio card</b>\n"
            "<i>Paste a 0x… address or the note / alias of a watched wallet. No /portfolio prefix.</i>"
        ),
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
        "poll_header": "<b>Predict.fun</b> <code>{addr}</code>{note}\n\n",
        "closed_item": "Closed: {key}",
        "fills_header": "<b>Order Fill</b> <code>{addr}</code>{note}\n\n",
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
        "btn_bulk_enter": "☑️ Multi-select",
        "btn_bulk_exit": "↩ Exit",
        "btn_bulk_all": "Select page",
        "btn_bulk_clear": "Clear",
        "btn_bulk_unwatch": "🛑 Unwatch ({n})",
        "btn_copy_addrs": "📋 Copy addrs",
        "copy_addrs_caption": "{count} addresses (long-press to copy)",
        "bulk_unwatch_done": "Unwatched {count} wallet(s).",
        "bulk_no_selection": "No wallets selected.",
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
        "rate_limited_alert": "🚦 Predict.fun rate-limited <code>{addr}</code>. Pausing {backoff}s; will auto-resume.",
        "rate_limit_recovered": "✅ Rate limit cleared for <code>{addr}</code>.",
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
        "poll_resolve_header": "🏁 <b>Market resolved</b> <code>{addr}</code>{note}\n\n",
        # --- Threshold / interval ---
        "usage_threshold": "Usage: /threshold addr|alias pct",
        "threshold_set": "🎚 Min-change filter for <code>{addr}</code>: {pct}%",
        "usage_interval": "Usage: /interval addr|alias seconds (0 = global)",
        "interval_set": "⏱ Poll interval for <code>{addr}</code>: {sec}s",
        "interval_too_small": "Interval must be 0 (global) or ≥ 5 seconds.",
        "usage_dustinterval": (
            "Usage: /dustinterval addr|alias seconds|off  "
            "(0 = global default, off = every poll, suffix m/h allowed)"
        ),
        "dustinterval_set": (
            "💨 Dust-summary interval for <code>{addr}</code>: {sec}s "
            "(0 = global default)"
        ),
        "dustinterval_off": (
            "💨 Dust batching disabled for <code>{addr}</code> — "
            "every poll fires a summary."
        ),
        "dustinterval_too_small": (
            "Dust interval must be 0 (global default), off, or ≥ 30 seconds."
        ),
        "usage_minfill": "Usage: /minfill addr|alias [usd]  (no amount → picker)",
        "minfill_set": "💨 Dust floor for <code>{addr}</code>: ${usd:.2f}",
        "minfill_bad_amount": "Amount must be ≥ 0.",
        "minfill_bad_number": "Reply must be a non-negative number (e.g. 1.5).",
        "dust_summary": "💨 {count} dust fills folded (${usd:,.2f} total)",
        "minfill_picker_title": "💨 <b>Micro-fill settings</b> · <code>{addr}</code>",
        "minfill_picker_floor": "💰 <b>Floor:</b> ${usd:.2f}{fallback} — fills below this fold into a summary",
        "minfill_picker_interval": "⏱ <b>Summary every:</b> {interval}{fallback}",
        "minfill_picker_fallback": " <i>(global default)</i>",
        "minfill_picker_hint": "Tap a preset to change — ✏️ for custom.",
        "minfill_preset_off": "$0 off",
        "btn_minfill_custom": "✏️",
        "minfill_custom_prompt": "Reply with a USD amount (e.g. <code>1.5</code>). 0 = off.",
        "btn_dust": "💨 Micro-fills",
        "btn_dustinterval_custom": "✏️",
        "btn_dustinterval_default": "🌐 Default",
        "dustinterval_preset_every_poll": "Every poll",
        "dustinterval_custom_prompt": (
            "Reply with an interval: "
            "<code>30m</code>, <code>8h</code>, <code>3600</code> (seconds), "
            "<code>off</code> = every poll, <code>default</code> = use global."
        ),
        "dustinterval_bad_input": (
            "Unrecognized. Try e.g. <code>30m</code>, <code>8h</code>, "
            "<code>off</code>, or <code>default</code>. Minimum custom value is 30s."
        ),
        "dustinterval_set_default": (
            "💨 <code>{addr}</code> will follow the global dust-summary default."
        ),
        "dust_label_every_poll": "every poll (no batching)",
        "dust_label_hours": "every {hours}h",
        "dust_label_minutes": "every {mins}m",
        "dust_label_seconds": "every {sec}s",
        "dust_btn_hours": "{hours}h",
        "dust_btn_minutes": "{mins}m",
        "dust_btn_seconds": "{sec}s",
        "btn_chatdef_open": "💨 Micro-fill defaults",
        "chatdef_picker_title": "💨 <b>Micro-fill defaults</b> (this chat)",
        "chatdef_picker_floor": "💰 <b>Default floor:</b> ${usd:.2f}{fallback} — applies to wallets that haven't set their own",
        "chatdef_picker_interval": "⏱ <b>Default summary every:</b> {interval}{fallback}",
        "chatdef_picker_fallback": " <i>(env fallback)</i>",
        "chatdef_picker_hint": "Wallet-level /minfill still overrides.",
        "chatdef_saved": "💨 Defaults saved.",
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
        "help_cmd_dustinterval": (
            "<b>/dustinterval</b> — Batch dust-fill summaries\n\n"
            "Sub-threshold fills accumulate and flush together once the window elapses, "
            "instead of firing a 💨 summary every poll. "
            "The default is <b>8 hours</b>; 0 falls back to the global default, "
            "<code>off</code> disables batching on that wallet (fires every poll), "
            "minimum when overridden is 30 seconds. Suffix m/h allowed.\n\n"
            "<b>Usage</b>\n"
            "• <code>/dustinterval alice 8h</code>  (one summary every 8 hours)\n"
            "• <code>/dustinterval alice 5m</code>  (one summary every 5 minutes)\n"
            "• <code>/dustinterval alice off</code>  (disable batching — every poll)\n"
            "• <code>/dustinterval alice 0</code>  (use the global default)"
        ),
        "help_cmd_settings": (
            "<b>/settings</b> — Preferences card\n\n"
            "Shows current language, global poll interval, watch count and muted count. "
            "Language buttons toggle EN/中文 in place."
        ),
        "help_cmd_defaults": (
            "<b>/defaults</b> — Chat-wide micro-fill defaults\n\n"
            "Set the default dust floor and dust-summary interval that apply to every "
            "wallet in this chat that hasn't been customized via the 💨 button on its "
            "watch card. Wallet-level overrides always win."
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
            "Default emits a <code>predict_watches.json</code> file with every watched address, note and muted flag. "
            "Use /import to restore it later.\n\n"
            "Pass a format to get a paste-friendly file instead:\n"
            "• <code>/export txt</code> — one address per line\n"
            "• <code>/export csv</code> — address,note,muted,positions,last_check"
        ),
        "help_cmd_import": (
            "<b>/import</b> — Bulk-import a watch list\n\n"
            "Reply to a <code>predict_watches.json</code> file (or any JSON document with the same shape) with /import "
            "to add the wallets in bulk. Duplicates are skipped. Max size 256 KB."
        ),
    },
    "zh": {
        "choose_lang": "🌐 选择语言：",
        "btn_watch_wallet": "➕ 关注钱包",
        "btn_watch_guide": "📖 如何找到钱包地址",
        "watch_reply_prompt": (
            "📝 <b>回复这条消息，发送要关注的钱包地址</b>\n"
            "<i>一行一个 0x 地址，可在地址后面跟备注（空格、- 或 ：分隔）。"
            "不用再输入 /watch。</i>"
        ),
        "watch_reply_invalid": (
            "❌ 这不是一个有效的钱包地址。\n\n"
            "请发送以 <code>0x</code> 开头的有效以太坊地址（42 个字符）。"
        ),
        "pos_reply_prompt": (
            "📊 <b>回复这条消息，发送要查询持仓的钱包地址</b>\n"
            "<i>0x 地址或已关注钱包的备注 / 别名都行，不用再输入 /pos。</i>"
        ),
        "orders_reply_prompt": (
            "📜 <b>回复这条消息，发送要查询最近成交的钱包地址</b>\n"
            "<i>0x 地址或已关注钱包的备注 / 别名都行，不用再输入 /orders。</i>"
        ),
        "portfolio_reply_prompt": (
            "📊 <b>回复这条消息，发送要查看资产卡片的钱包地址</b>\n"
            "<i>0x 地址或已关注钱包的备注 / 别名都行，不用再输入 /portfolio。</i>"
        ),
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
        "poll_header": "<b>Predict.fun</b> <code>{addr}</code>{note}\n\n",
        "closed_item": "已平仓：{key}",
        "fills_header": "<b>订单成交</b> <code>{addr}</code>{note}\n\n",
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
        "btn_bulk_enter": "☑️ 多选",
        "btn_bulk_exit": "↩ 退出多选",
        "btn_bulk_all": "本页全选",
        "btn_bulk_clear": "清空",
        "btn_bulk_unwatch": "🛑 取消选中 ({n})",
        "btn_copy_addrs": "📋 复制地址",
        "copy_addrs_caption": "{count} 个地址（长按复制）",
        "bulk_unwatch_done": "已取消监控 {count} 个钱包。",
        "bulk_no_selection": "未选择任何钱包。",
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
        "rate_limited_alert": "🚦 Predict.fun 限流：<code>{addr}</code>，暂停 {backoff} 秒后自动恢复。",
        "rate_limit_recovered": "✅ <code>{addr}</code> 限流已恢复。",
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
        "poll_resolve_header": "🏁 <b>市场已结算</b> <code>{addr}</code>{note}\n\n",
        "usage_threshold": "用法：/threshold 地址或备注 百分比",
        "threshold_set": "🎚 <code>{addr}</code> 最小变动过滤：{pct}%",
        "usage_interval": "用法：/interval 地址或备注 秒（0 = 使用全局）",
        "interval_set": "⏱ <code>{addr}</code> 轮询间隔：{sec} 秒",
        "interval_too_small": "间隔必须为 0（使用全局）或 ≥ 5 秒。",
        "usage_dustinterval": (
            "用法：/dustinterval 地址或备注 秒|off"
            "（0 = 使用全局默认，off = 每次轮询都发，可加 m/h 后缀）"
        ),
        "dustinterval_set": (
            "💨 <code>{addr}</code> 小额汇总间隔：{sec} 秒（0 = 全局默认）"
        ),
        "dustinterval_off": (
            "💨 <code>{addr}</code> 已关闭小额节流——每次轮询都发汇总。"
        ),
        "dustinterval_too_small": "小额汇总间隔必须为 0（全局默认）、off，或 ≥ 30 秒。",
        "usage_minfill": "用法：/minfill 地址或备注 [金额]（不填金额弹选择器）",
        "minfill_set": "💨 <code>{addr}</code> 小额阈值：${usd:.2f}",
        "minfill_bad_amount": "金额必须 ≥ 0。",
        "minfill_bad_number": "回复必须是非负数字（例如 1.5）。",
        "dust_summary": "💨 今次有 {count} 笔小额，共 ${usd:,.2f}",
        "minfill_picker_title": "💨 <b>小额提醒设置</b> · <code>{addr}</code>",
        "minfill_picker_floor": "💰 <b>成交额小于多少美元不单独提醒：</b>${usd:.2f}{fallback}",
        "minfill_picker_interval": "⏱ <b>这些小额多久合并提醒一次：</b>{interval}{fallback}",
        "minfill_picker_fallback": "<i>（跟随默认）</i>",
        "minfill_picker_hint": (
            "例：金额 $1、间隔 8 小时 = 小于 $1 的成交不立刻提醒，"
            "攒够 8 小时合并成一条「💨 今次有 N 笔小额，共 $X」。\n"
            "点按钮改，✏️ 自定义。"
        ),
        "minfill_preset_off": "🌐 默认",
        "btn_minfill_custom": "✏️",
        "minfill_custom_prompt": "回复一个美元数（例如 <code>1.5</code>）。填 0 = 跟随默认。",
        "btn_dust": "💨 小额提醒",
        "btn_dustinterval_custom": "✏️",
        "btn_dustinterval_default": "🌐 默认",
        "dustinterval_preset_every_poll": "每笔都发",
        "dustinterval_custom_prompt": (
            "回复一个间隔："
            "<code>30m</code>、<code>8h</code>、<code>3600</code>（秒）、"
            "<code>off</code>（每次都发）、<code>default</code>（回到全局默认）。"
        ),
        "dustinterval_bad_input": (
            "看不懂。试试 <code>30m</code>、<code>8h</code>、"
            "<code>off</code> 或 <code>default</code>，自定义数值最少 30 秒。"
        ),
        "dustinterval_set_default": (
            "💨 <code>{addr}</code> 已改回跟随全局小额汇总默认。"
        ),
        "dust_label_every_poll": "不合并，每笔都发",
        "dust_label_hours": "每 {hours} 小时一次",
        "dust_label_minutes": "每 {mins} 分钟一次",
        "dust_label_seconds": "每 {sec} 秒一次",
        "dust_btn_hours": "{hours} 小时",
        "dust_btn_minutes": "{mins} 分钟",
        "dust_btn_seconds": "{sec} 秒",
        "btn_chatdef_open": "💨 小额提醒默认",
        "chatdef_picker_title": "💨 <b>小额提醒 · 本聊天默认</b>",
        "chatdef_picker_floor": "💰 <b>成交额小于多少美元不单独提醒：</b>${usd:.2f}{fallback}",
        "chatdef_picker_interval": "⏱ <b>这些小额多久合并提醒一次：</b>{interval}{fallback}",
        "chatdef_picker_fallback": "<i>（当前使用系统内置值）</i>",
        "chatdef_picker_hint": (
            "例：金额 $1、间隔 8 小时 = 小于 $1 的成交不立刻提醒，"
            "攒够 8 小时合并成一条「💨 今次有 N 笔小额，共 $X」。\n\n"
            "这是本聊天所有钱包的默认；单钱包在 /list 的 💨 按钮里"
            "自己调过的会优先。点按钮改，✏️ 自定义。"
        ),
        "chatdef_saved": "💨 默认已保存。",
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
        "help_cmd_dustinterval": (
            "<b>/dustinterval</b> — 小额汇总节流\n\n"
            "低于阈值的小额成交会累积在内存里，到达间隔后才合并成一条 💨 汇总提醒，"
            "不再每次轮询都打扰。默认 <b>8 小时</b>；填 0 = 使用全局默认；"
            "填 <code>off</code> = 该钱包彻底关掉节流（每次轮询都发）；"
            "自定义时最小 30 秒；可加 m/h 后缀。\n\n"
            "<b>用法</b>\n"
            "• <code>/dustinterval 张三 8h</code>（每 8 小时汇总一次）\n"
            "• <code>/dustinterval 张三 5m</code>（每 5 分钟汇总一次）\n"
            "• <code>/dustinterval 张三 off</code>（关闭节流，每次轮询都发）\n"
            "• <code>/dustinterval 张三 0</code>（回到全局默认）"
        ),
        "help_cmd_settings": (
            "<b>/settings</b> — 偏好面板\n\n"
            "显示当前语言、全局轮询间隔、监控数和静音数，下面有语言切换按钮。"
        ),
        "help_cmd_defaults": (
            "<b>/defaults</b> — 小额提醒默认\n\n"
            "<b>小于某个美元的成交不单独提醒</b>，而是攒起来每隔一段时间"
            "合并成一条「💨 今次有 N 笔小额，共 $X」，免得刷屏。\n\n"
            "这里设两个数字：\n"
            "• <b>多小算小额</b>（例：$1 = 交易额 &lt; $1 的成交不单独提醒）\n"
            "• <b>多久合并一次</b>（例：8 小时 = 攒 8 小时发一条汇总）\n\n"
            "设好后，本聊天所有没单独调过的钱包都跟这个默认。"
            "想给某个钱包单独调，去 /list 点它的 💨 按钮。"
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
            "默认生成 <code>predict_watches.json</code>，包含每个钱包的地址、备注、静音状态。可以用 /import 恢复。\n\n"
            "也可以指定格式得到便于复制 / 导入其他工具的文件：\n"
            "• <code>/export txt</code> — 每行一个地址\n"
            "• <code>/export csv</code> — address,note,muted,positions,last_check"
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
                min_fill_usd REAL NOT NULL DEFAULT 0,
                dust_interval_s INTEGER NOT NULL DEFAULT 0,
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
            "ALTER TABLE watches ADD COLUMN min_fill_usd REAL NOT NULL DEFAULT 0",
            "ALTER TABLE watches ADD COLUMN dust_interval_s INTEGER NOT NULL DEFAULT 0",
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
        # Chat-level defaults for the micro-fill picker. These sit between
        # per-wallet overrides and the global env fallbacks: wallets whose
        # own value is "inherit" (min_fill_usd==0 / dust_interval_s==0)
        # pick these up before falling back to MIN_FILL_USD / DUST_INTERVAL.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_defaults (
                chat_id INTEGER PRIMARY KEY,
                default_min_fill_usd REAL NOT NULL DEFAULT 0,
                default_dust_interval_s INTEGER NOT NULL DEFAULT 0
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
              poll_interval_s, last_activity, resolved_markets, min_fill_usd,
              dust_interval_s
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
              resolved_markets=excluded.resolved_markets,
              min_fill_usd=excluded.min_fill_usd,
              dust_interval_s=excluded.dust_interval_s
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
                float(w.min_fill_usd or 0),
                int(w.dust_interval_s or 0),
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


def save_chat_defaults(chat_id: int):
    """Persist the in-memory chat_defaults entry for this chat."""
    row = chat_defaults.get(chat_id, {})
    min_fill = float(row.get("min_fill_usd", 0) or 0)
    dust_interval = int(row.get("dust_interval_s", 0) or 0)
    with db_lock, db_conn() as conn:
        conn.execute(
            """
            INSERT INTO chat_defaults(chat_id, default_min_fill_usd, default_dust_interval_s)
            VALUES(?,?,?)
            ON CONFLICT(chat_id) DO UPDATE SET
                default_min_fill_usd=excluded.default_min_fill_usd,
                default_dust_interval_s=excluded.default_dust_interval_s
            """,
            (chat_id, min_fill, dust_interval),
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

        try:
            for cid, min_fill, dust_int in conn.execute(
                "SELECT chat_id, default_min_fill_usd, default_dust_interval_s FROM chat_defaults"
            ):
                chat_defaults[int(cid)] = {
                    "min_fill_usd": float(min_fill or 0),
                    "dust_interval_s": int(dust_int or 0),
                }
        except sqlite3.OperationalError:
            pass

        for row in conn.execute(
            """
            SELECT chat_id, address, note, position_snapshot, order_match_snapshot,
                   last_check, muted, position_titles, change_threshold_pct,
                   poll_interval_s, last_activity, resolved_markets, min_fill_usd,
                   dust_interval_s
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
                min_fill_usd,
                dust_interval_s,
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
                min_fill_usd=float(min_fill_usd or 0),
                dust_interval_s=int(dust_interval_s or 0),
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


@dataclass
class RateLimited:
    """Sentinel returned by fetch_* helpers when the API responded with 429.

    Carries the parsed Retry-After hint (seconds) when the server provided one.
    The polling loop uses this to set a per-wallet cooldown.
    """
    retry_after_s: float | None = None


def _record_api(endpoint: str, label: str) -> None:
    """Append a (timestamp, label) entry to the rolling api_stats deque."""
    bucket = api_stats.get(endpoint)
    if bucket is not None:
        bucket.append((time.time(), label))


def _parse_retry_after(value: str | None) -> float | None:
    """Parse an HTTP Retry-After header value (seconds or HTTP-date)."""
    if not value:
        return None
    value = value.strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    return max(0.0, dt.timestamp() - time.time())


async def fetch_positions(
    session: aiohttp.ClientSession, address: str
) -> list[dict] | RateLimited | None:
    """Return positions list, ``RateLimited`` on 429, or ``None`` on other errors.

    An empty list means the wallet genuinely has no positions; ``None`` lets
    callers track consecutive failures for alerting; a ``RateLimited`` instance
    signals the caller should back off polling for this wallet.
    """
    url = f"{PREDICT_API}/v1/positions/{address}"
    try:
        async with session.get(
            url,
            headers=_headers(),
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status == 200:
                _record_api("positions", "ok")
                data = await resp.json()
                return data.get("data", []) if data.get("success") else []

            if resp.status == 429:
                _record_api("positions", "429")
                logger.warning(f"Positions API 429 (rate limited) for {address}")
                return RateLimited(_parse_retry_after(resp.headers.get("Retry-After")))

            label = "5xx" if resp.status >= 500 else "other"
            _record_api("positions", label)
            logger.warning(f"Positions API {resp.status} for {address}")
            return None
    except asyncio.TimeoutError:
        _record_api("positions", "timeout")
        logger.error(f"fetch_positions timeout for {address}")
        return None
    except Exception as e:
        _record_api("positions", "timeout")
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
) -> list[dict] | RateLimited | None:
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
                _record_api("matches", "ok")
                data = await resp.json()
                return data.get("data", []) if data.get("success") else []

            if resp.status == 429:
                _record_api("matches", "429")
                logger.warning(f"Orders matches API 429 (rate limited) for {address}")
                return RateLimited(_parse_retry_after(resp.headers.get("Retry-After")))

            label = "5xx" if resp.status >= 500 else "other"
            _record_api("matches", label)
            logger.warning(f"Orders matches API {resp.status} for {address}")
            return None
    except asyncio.TimeoutError:
        _record_api("matches", "timeout")
        logger.error(f"fetch_order_matches timeout for {address}")
        return None
    except Exception as e:
        _record_api("matches", "timeout")
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
    # predict.fun's public URL is /market/<category_slug> (e.g.
    # english-premier-league-winner). The plain ``id`` fallback produces
    # /market/1565 which hits a placeholder page, so only fall back to it
    # when no slug-like field is available.
    slug = (
        market.get("slug")
        or market.get("marketSlug")
        or market.get("categorySlug")
        or market.get("category_slug")
        or market.get("id")
    )
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
    # predict.fun returns prices as 18-decimal probability fractions
    # (e.g. 2000000000000000 → 0.002 → 0.2¢). Decimal-string prices
    # like "0.002" or "0.997" skip the scaling step and go straight to ×100.
    if abs(p) >= 1e6:
        p = p / 1e18
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


def _fmt_shares(v) -> str:
    """Share count with scale-adaptive precision.

    Large holdings don't need four decimals — "76,422" reads cleaner than
    "76421.9982". Fractional stakes keep their precision so a 1.04-share fill
    is still distinguishable from 1.05.
    """
    if v is None:
        return "?"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return "?"
    if abs(f) >= 1000:
        return f"{f:,.0f}"
    if abs(f) >= 100:
        return f"{f:,.1f}".rstrip("0").rstrip(".")
    return _fmt_num(f, digits=4)


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
    )
    price_c = _norm_price_to_cents(price_raw)

    # If no explicit entry/avg field is available, derive the cost basis from
    # the USD cost recorded for the position. Done before falling back to the
    # current mark price so 总持仓 reflects what the wallet actually paid,
    # not the (possibly drifted) market quote.
    if price_c is None:
        cost_usd = _safe_float(
            pos.get("costUsd")
            or pos.get("cost_usd")
            or pos.get("totalCost")
            or pos.get("totalCostUsd")
            or pos.get("initialValue")
            or pos.get("initialValueUsd")
        )
        if cost_usd is not None and shares_num and shares_num > 0:
            price_c = cost_usd / shares_num * 100

    # Mark-price fallbacks — only used when neither an explicit cost-basis
    # field nor a USD cost total is available from the API.
    if price_c is None:
        mark_raw = pos.get("currentPrice") or outcome_obj.get("price")
        if mark_raw is None and idx is not None:
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
                    mark_raw = prices[i]
            except (ValueError, TypeError):
                pass
        price_c = _norm_price_to_cents(mark_raw)

    # Last-resort: derive from a generic USD value field (may be mark value
    # rather than cost). Accepted only when every other signal is missing so
    # we at least render *something* instead of "?".
    if price_c is None:
        value_usd = _safe_float(pos.get("valueUsd") or pos.get("value_usd"))
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


def _title_cache_entry(pos: dict, market: dict | None = None) -> dict:
    """Build the per-position cache record used for 已平仓 notifications.

    Stores title + outcome + slug so the 已平仓 card can still render the
    outcome label and a clickable market link after the position disappears
    from /v1/positions.
    """
    title, outcome, _, _ = display_fields(pos, market or {})
    combined = market or pos.get("_market") or {}
    nested = pos.get("market") if isinstance(pos.get("market"), dict) else {}
    slug = (
        combined.get("slug")
        or combined.get("marketSlug")
        or combined.get("categorySlug")
        or combined.get("category_slug")
        or nested.get("slug")
        or nested.get("marketSlug")
        or nested.get("categorySlug")
        or nested.get("category_slug")
    )
    return {
        "title": (title or "")[:80],
        "outcome": outcome or "",
        "slug": slug,
    }


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
    match: dict | None = None,
) -> str:
    title, outcome, shares, avg = display_fields(pos, market)
    title = title[:55]

    # Use the API's own USD cost when exposed — avoids drift from rounding
    # `avg` to 2dp when the real avg has more precision (e.g. 0.333…¢).
    cost_usd_raw = _safe_float(
        pos.get("costUsd")
        or pos.get("cost_usd")
        or pos.get("totalCost")
        or pos.get("totalCostUsd")
        or pos.get("initialValue")
        or pos.get("initialValueUsd")
    )
    if cost_usd_raw is not None:
        val = f"${cost_usd_raw:,.2f}"
    else:
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
    try:
        new_num = float(shares)
    except (ValueError, TypeError):
        new_num = None
    shares_disp = _fmt_shares(new_num) if new_num is not None else shares
    size_line = f"💹 {t(chat_id, 'label_size')}: <code>{shares_disp}</code> {t(chat_id, 'shares_unit')} @ <code>{avg}c</code>"
    if label == "changed" and prev_shares is not None and new_num is not None:
        delta = new_num - prev_shares
        arrow = "▲" if delta > 0 else ("▼" if delta < 0 else "=")
        prev_text = _fmt_shares(prev_shares)
        delta_text = _fmt_shares(abs(delta))
        size_line = (
            f"💹 {t(chat_id, 'label_size')}: <code>{prev_text}</code> → "
            f"<code>{shares_disp}</code> {t(chat_id, 'shares_unit')} "
            f"({arrow}{delta_text}) @ <code>{avg}c</code>"
        )

    combined_market = market or pos.get("_market")
    # Fall back to the position's nested ``market`` dict so the title link
    # still renders when /v1/markets/{id} returned nothing (empty _market).
    nested_market = pos.get("market") if isinstance(pos.get("market"), dict) else {}
    if isinstance(combined_market, dict):
        link_market = {**nested_market, **combined_market}
    else:
        link_market = nested_market
    pnl_usd, pnl_pct, _ = pnl_of(pos, combined_market)
    pnl_line = _pnl_line(chat_id, pnl_usd, pnl_pct) if label != "closed" else ""

    tx_line = ""
    if match:
        tx = match.get("transactionHash")
        if isinstance(tx, str) and tx.startswith("0x"):
            tx_url = f"{TX_EXPLORER_BASE.rstrip('/')}/{tx}"
            tx_line = f'\n🔗 <a href="{tx_url}">{t(chat_id, "view_tx")}</a>'

    return (
        f"{emoji} <b>{_title_html(title, link_market)}</b>\n"
        f"✅ <b>{outcome}</b>\n"
        f"{size_line}\n"
        f"💰 {t(chat_id, 'label_value')}: <b>{val}</b>"
        f"{pnl_line}"
        f"{tx_line}"
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


def _side_text(side: str, chat_id: int) -> str:
    s = (side or "").lower()
    if s == "bid":
        return t(chat_id, "side_bid")
    if s == "ask":
        return t(chat_id, "side_ask")
    return side or "?"


def _position_lookup_key(market_id, outcome_index) -> str:
    return f"{market_id or ''}_{outcome_index if outcome_index is not None else ''}"


def _pick_user_side(match: dict, address: str | None) -> dict:
    """Return the side (maker entry or taker) belonging to ``address``.

    predict.fun's matches API reports each fill with two complementary
    sides — our user is usually the maker (listed in ``makers[]``) but can
    also be the taker. For neg-risk markets, the two sides can even hold
    different outcomes (user sells Yes at 0.3¢ while counter-taker sells No
    at 99.7¢ and they merge). We must identify OUR side by ``signer`` so the
    quoteType/outcome/price reflect the user's actual trade — not the
    complementary leg.

    Falls back to the taker when no address is provided or no side matches,
    preserving the pre-fix behavior as a safety net.
    """
    taker = match.get("taker") if isinstance(match.get("taker"), dict) else {}
    makers = match.get("makers") if isinstance(match.get("makers"), list) else []
    if address:
        addr_lower = address.lower()
        for m in makers:
            if not isinstance(m, dict):
                continue
            if str(m.get("signer") or "").lower() == addr_lower:
                return m
        if str(taker.get("signer") or "").lower() == addr_lower:
            return taker
    # No match — fall back to taker, or first maker if taker missing.
    if taker:
        return taker
    return makers[0] if makers and isinstance(makers[0], dict) else {}


def _fill_usd_value(match: dict, address: str | None) -> float | None:
    """USD notional of a fill from the user's perspective.

    Mirrors the shares × price math inside ``fmt_match`` so the poll loop can
    cheaply filter dust fills before formatting.
    """
    our_side = _pick_user_side(match, address)
    shares = _norm_amount_to_shares(match.get("amountFilled"))
    if shares is None:
        shares = _norm_amount_to_shares(our_side.get("amount"))
    price_c = _norm_price_to_cents(our_side.get("price"))
    if price_c is None:
        price_c = _norm_price_to_cents(match.get("priceExecuted"))
    if shares is None or price_c is None:
        return None
    return shares * price_c / 100


def _match_market_view(match: dict) -> dict:
    """Return the best market dict for a trade record.

    The orders API returns two shapes depending on endpoint/version: a nested
    ``match["market"]`` dict (carrying an id plus sometimes a generic category
    title like "Match Winner") and/or flat per-trade fields
    ``market_id`` / ``market_title`` / ``category_slug``. The flat fields are
    the ones that line up with predict.fun's public URL
    (``/market/<category_slug>``) and with the specific item the trade was
    against (e.g. "Chelsea"), so we prefer them for title/slug when set,
    falling back to the nested dict otherwise. Never mutates input.
    """
    nested = match.get("market") if isinstance(match.get("market"), dict) else {}
    out = dict(nested)
    flat_id = match.get("market_id")
    if flat_id is not None and not out.get("id"):
        out["id"] = flat_id
    flat_title = match.get("market_title")
    if flat_title:
        out["title"] = flat_title
    flat_slug = match.get("category_slug") or match.get("categorySlug")
    if flat_slug:
        out["slug"] = flat_slug
    return out


def _fill_market_outcome_key(match: dict) -> str | None:
    """Stable ``marketId_outcomeIndex`` key to match fills against positions."""
    market = _match_market_view(match)
    market_id = market.get("id") or ""
    taker = match.get("taker") if isinstance(match.get("taker"), dict) else {}
    outcome_obj = None
    if isinstance(taker.get("outcome"), dict):
        outcome_obj = taker["outcome"]
    else:
        makers = match.get("makers") if isinstance(match.get("makers"), list) else []
        for m in makers:
            if isinstance(m, dict) and isinstance(m.get("outcome"), dict):
                outcome_obj = m["outcome"]
                break
    idx = outcome_obj.get("index") if outcome_obj else None
    if not market_id and idx is None:
        return None
    return _position_lookup_key(market_id, idx)


def fmt_match(
    match: dict,
    chat_id: int,
    positions_by_key: dict | None = None,
    address: str | None = None,
) -> str:
    market = _match_market_view(match)

    our_side = _pick_user_side(match, address)
    outcome_obj = our_side.get("outcome") if isinstance(our_side.get("outcome"), dict) else {}

    title = market.get("title") or market.get("question") or str(market.get("id", "?"))
    outcome = (
        outcome_obj.get("name")
        or match.get("outcome_name")
        or match.get("outcomeName")
        or "?"
    )
    quote_type = (our_side.get("quoteType") or "").lower()
    side = _side_text(our_side.get("quoteType", "?"), chat_id)
    # Color the side to match the rest of the UI (🟢 gain / 🔴 loss). Bid adds
    # exposure so it mirrors "new position green"; ask reduces it like "closed
    # red". Falls back to a neutral marker for unknown quoteTypes.
    side_emoji = "🟢" if quote_type == "bid" else ("🔴" if quote_type == "ask" else "⚪")
    # Prefer the top-level amountFilled so merged fills (consolidate_fills sets
    # amountFilled to the summed shares) display the total, not just the first
    # partial. For non-merged matches, amountFilled equals our_side.amount.
    shares = _norm_amount_to_shares(match.get("amountFilled"))
    if shares is None:
        shares = _norm_amount_to_shares(our_side.get("amount"))
    shares_text = _fmt_shares(shares)

    # Price/value come directly from the user's own side entry. ``price`` is
    # an 18-decimal probability fraction (e.g. 2000000000000000 → 0.002 →
    # 0.2¢). Value is shares × price — what the user paid (Bid) or received
    # (Ask), before fees.
    price_c = _norm_price_to_cents(our_side.get("price"))
    if price_c is None:
        # Very old cached fills without a side.price still fall back to the
        # top-level priceExecuted, which always reflects the taker's price.
        price_c = _norm_price_to_cents(match.get("priceExecuted"))
    fill_value_usd = None
    if shares is not None and price_c is not None:
        fill_value_usd = shares * price_c / 100
        value_text = f"${fill_value_usd:,.2f}"
    else:
        value_text = "N/A"

    price_text = f"{price_c:.2f}" if price_c is not None else "?"
    tx = match.get("transactionHash") or "N/A"
    if isinstance(tx, str) and tx.startswith("0x"):
        tx_url = f"{TX_EXPLORER_BASE.rstrip('/')}/{tx}"
        tx_line = f'🔗 <a href="{tx_url}">{t(chat_id, "view_tx")}</a>'
    else:
        tx_short = f"{tx[:10]}...{tx[-6:]}" if isinstance(tx, str) and len(tx) > 20 else str(tx)
        tx_line = f"🔗 <code>{tx_short}</code>"

    # If we have the wallet's current positions, surface the total holding for
    # this market+outcome so the user can distinguish "this fill" from "what
    # they now hold in total" — especially when adding to an existing stake.
    # Also computes the before→after delta and floating PnL so a single fill
    # line subsumes the separate 持仓变化 notification.
    total_line = ""
    pnl_line = ""
    if positions_by_key:
        market_id = market.get("id") or ""
        outcome_idx = outcome_obj.get("index")
        lookup_key = _position_lookup_key(market_id, outcome_idx)
        current_pos = positions_by_key.get(lookup_key)
        if current_pos:
            total_shares = pos_size(current_pos)
            _, _, _, total_price = display_fields(
                current_pos, current_pos.get("_market")
            )
            total_shares_text = _fmt_shares(total_shares)
            # Prefer the API's own USD cost for the total, so rounding the
            # displayed avg price to 2dp (e.g. 0.33¢ for 0.333…¢) doesn't
            # drift the total by several dollars on large positions.
            cost_usd_raw = _safe_float(
                current_pos.get("costUsd")
                or current_pos.get("cost_usd")
                or current_pos.get("totalCost")
                or current_pos.get("totalCostUsd")
                or current_pos.get("initialValue")
                or current_pos.get("initialValueUsd")
            )
            if cost_usd_raw is not None:
                total_val = f"${cost_usd_raw:,.2f}"
            else:
                try:
                    total_val = f"${float(total_shares) * float(total_price) / 100:,.2f}" if total_shares is not None else "N/A"
                except (ValueError, TypeError):
                    total_val = "N/A"
            # Show avg-cost vs live mark when they diverge (>2% gap) so users
            # can see unrealized drift without opening /pos.
            price_detail = f"<code>{total_price}c</code>"
            pnl_usd, pnl_pct, mark_c = pnl_of(current_pos, current_pos.get("_market"))
            try:
                avg_c = float(total_price)
            except (ValueError, TypeError):
                avg_c = None
            if avg_c and mark_c and avg_c > 0 and abs(mark_c - avg_c) / avg_c > 0.02:
                price_detail = f"<code>{total_price}c</code> → <code>{mark_c:.2f}c</code>"
            if total_shares is not None:
                total_line = (
                    f"\n📦 {t(chat_id, 'label_total_pos')}: "
                    f"<b>{total_shares_text}</b> {t(chat_id, 'shares_unit')} "
                    f"@ {price_detail} = <b>{total_val}</b>"
                )
            pnl_line = _pnl_line(chat_id, pnl_usd, pnl_pct)

    merged_suffix = ""
    merged_count = match.get("_merged_count")
    if isinstance(merged_count, int) and merged_count > 1:
        merged_suffix = t(chat_id, "fills_merged_suffix", count=merged_count)

    title_html = _title_html(title[:50], market)
    return (
        f"{side_emoji} {side} {outcome} | "
        f"{t(chat_id, 'label_fill')} {shares_text} @ {price_text}c = {value_text}{merged_suffix}\n"
        f"{title_html}{total_line}{pnl_line}\n"
        f"{tx_line}"
    )

# ==================== Telegram ====================


def _start_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🇺🇸 English", callback_data="lang_en"),
                InlineKeyboardButton("🇨🇳 中文", callback_data="lang_zh"),
            ],
            [InlineKeyboardButton(t(chat_id, "btn_watch_wallet"), callback_data="watch_prompt")],
            [InlineKeyboardButton(t(chat_id, "btn_watch_guide"), callback_data="watch_guide")],
            [
                InlineKeyboardButton(
                    t(chat_id, "btn_chatdef_open"), callback_data="cdef:open"
                ),
                InlineKeyboardButton(t(chat_id, "btn_help"), callback_data="help_root"),
            ],
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
    if positions is None or isinstance(positions, RateLimited):
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
        if isinstance(matches, RateLimited):
            matches = None
        positions, positions_by_key = await _fetch_positions_with_markets(session, addr)
        # The orders API returns a stub market on each match (sometimes just
        # an id, sometimes a flat market_id/market_title/category_slug trio),
        # which leaves market_url() without a proper slug and produces
        # /market/{uuid} links that 404. Backfill with the cached
        # /v1/markets/{id} payload so title + slug line up with the page the
        # user actually traded.
        for m in matches or []:
            view = _match_market_view(m)
            mid = view.get("id")
            fresh = await fetch_market(session, mid) if mid else {}
            merged = {**view, **(fresh or {})}
            if view.get("title"):
                merged["title"] = view["title"]
            if view.get("slug"):
                merged["slug"] = view["slug"]
            if merged:
                m["market"] = merged

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

    lines = [t(chat_id, "orders_header", addr=addr, count=len(matches)), ""]
    lines.extend(fmt_match(m, chat_id, positions_by_key, address=addr) for m in chunk)

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


async def _prompt_watch_reply(ctx: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Ask the user to reply with a wallet address to watch."""
    _set_pending_watch(ctx, chat_id)
    await ctx.bot.send_message(
        chat_id=chat_id,
        text=t(chat_id, "watch_reply_prompt"),
        parse_mode="HTML",
        reply_markup=ForceReply(selective=True),
    )


async def _prompt_addr_reply(
    ctx: ContextTypes.DEFAULT_TYPE, chat_id: int, kind: str
):
    """Ask the user to reply with a wallet address for a query command.

    `kind` is one of "pos" / "orders" / "portfolio" — picked up by on_message
    to route the reply back into the matching cmd_* handler. Mirrors the
    /watch reply flow so users don't have to retype the slash command.
    """
    ctx.user_data["pending_addr_query"] = {"chat_id": chat_id, "kind": kind}
    await ctx.bot.send_message(
        chat_id=chat_id,
        text=t(chat_id, f"{kind}_reply_prompt"),
        parse_mode="HTML",
        reply_markup=ForceReply(selective=True),
    )


_NOTE_SEP_CHARS = "-—–:：·｜|,，;；"
_ADDR_RE = re.compile(r"0x[0-9a-fA-F]{40}")


def _parse_watch_pairs(text: str) -> list[tuple[str, str]]:
    """Parse /watch (or watch-reply) input into (address, note) pairs.

    Rule: **one line = one address**. For every non-blank line we pull the
    first 0x… address off the line and treat everything after it as the
    note. Duplicate addresses within the same input (case-insensitive) are
    folded together so pasting the same wallet twice won't double-count.

    Supported shapes:
      - "<addr>"                              → 1 pair, no note
      - "<addr> <note words>"                 → 1 pair with note
      - Multi-line "<addr>[ sep note]" per    → N pairs, one per line
        line (common separators - — – : ： · ｜ | , ， ; ； are trimmed
        off the note).

    Back-compat: a single-line "<addr1> <addr2> …" paste (the legacy
    multi-address form) is still accepted — any extra 0x addresses on
    that one line each become their own pair with an empty note.
    """
    if not text:
        return []
    text = text.lstrip()
    # Drop a leading slash command ("/watch ..." or "/watch@bot ...") on
    # the first line so the first address isn't shadowed by it.
    if text.startswith("/"):
        head, _, tail = text.partition("\n")
        head_after = head.split(maxsplit=1)
        head_remainder = head_after[1] if len(head_after) > 1 else ""
        text = head_remainder + (("\n" + tail) if tail else "")

    def _strip_note(raw: str) -> str:
        raw = raw.strip()
        # Eat any leading separator chars (and the whitespace after them) so
        # "addr - 备注" / "addr：备注" / "addr | 备注" all yield "备注".
        while raw and raw[0] in _NOTE_SEP_CHARS:
            raw = raw[1:].lstrip()
        return raw

    pairs: list[tuple[str, str]] = []
    seen: set[str] = set()

    lines = [ln for ln in text.splitlines() if ln.strip()]
    is_multi_line = len(lines) > 1
    for line in lines:
        matches = list(_ADDR_RE.finditer(line))
        if not matches:
            continue
        first = matches[0]
        addr = first.group(0)
        # Note = the slice between this address and the next one on the
        # same line (so a stray second address never leaks into the note
        # text). EOL when there's only one address on the line.
        if len(matches) > 1:
            note_slice = line[first.end() : matches[1].start()]
        else:
            note_slice = line[first.end() :]
        note = _strip_note(note_slice)

        key = addr.lower()
        if key not in seen:
            seen.add(key)
            pairs.append((addr, note))

        # Extra 0x addresses on a single-line paste → each gets its own
        # pair with an empty note. Ignored on multi-line pastes to honor
        # the one-address-per-line rule.
        if not is_multi_line:
            for m in matches[1:]:
                extra = m.group(0)
                ek = extra.lower()
                if ek in seen:
                    continue
                seen.add(ek)
                pairs.append((extra, ""))

    return pairs


async def cmd_watch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if not ctx.args:
        # No args — show the full visual guide (image + numbered steps +
        # format examples) so new users still learn how to find their
        # wallet address, then follow up with a compact ForceReply so
        # experienced users can just paste the address and go.
        await _send_watch_guide(ctx, chat_id)
        await _prompt_watch_reply(ctx, chat_id)
        return

    # Parse the raw message text — using ctx.args alone collapses multi-line
    # input ("addr - note" per line) into a flat token stream, which used to
    # silently keep only the first address.
    pairs = _parse_watch_pairs(update.message.text or "")

    def _is_addr(a: str) -> bool:
        return a.startswith("0x") and len(a) == 42

    # Fallback for code paths that hand us ctx.args without a real message
    # body (callbacks, programmatic invocations).
    if not pairs and ctx.args:
        raw_args = [a.strip() for a in ctx.args if a.strip()]
        if raw_args:
            if len(raw_args) > 1 and all(_is_addr(a) for a in raw_args):
                pairs = [(a, "") for a in raw_args]
            else:
                addr = raw_args[0]
                note = " ".join(raw_args[1:]).strip()
                while note and note[0] in _NOTE_SEP_CHARS:
                    note = note[1:].lstrip()
                pairs = [(addr, note)]

    if not pairs:
        await update.message.reply_text(
            t(chat_id, "invalid_address"), parse_mode="HTML"
        )
        return

    multi_mode = len(pairs) > 1
    first_addr, first_note = pairs[0]

    # Duplicate-note handling (T12): single-address case where the address
    # is already watched and the supplied note differs — ask before
    # overwriting instead of silently skipping.
    if (
        not multi_mode
        and first_note
        and _is_addr(first_addr)
        and first_addr in watched.get(chat_id, {})
        and watched[chat_id][first_addr].note != first_note
    ):
        existing = watched[chat_id][first_addr]
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        t(chat_id, "btn_overwrite_note"),
                        callback_data=f"watchover:{first_addr}:{first_note[:50]}",
                    ),
                    InlineKeyboardButton(
                        t(chat_id, "btn_cancel"),
                        callback_data=f"watchover:{first_addr}:__CANCEL__",
                    ),
                ]
            ]
        )
        await update.message.reply_text(
            t(
                chat_id,
                "dup_watch_prompt",
                addr=fmt_addr(first_addr),
                old=_html_escape(existing.note or "(none)"),
                new=_html_escape(first_note),
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
        for addr, note in pairs:
            if not _is_addr(addr):
                skipped += 1
                continue
            if addr.lower() in {a.lower() for a in watched[chat_id]}:
                skipped += 1
                continue

            positions = await fetch_positions(session, addr)
            matches = await fetch_order_matches(session, addr, first=30)
            if isinstance(positions, RateLimited):
                positions = None
            if isinstance(matches, RateLimited):
                matches = None
            if positions is None and matches is None:
                # Treat as skipped — can't establish a snapshot reliably.
                skipped += 1
                continue
            positions = positions or []
            matches = matches or []

            snapshot = {pos_key(p): pos_size(p) for p in positions}
            title_cache = {pos_key(p): _title_cache_entry(p) for p in positions}
            watched[chat_id][addr] = WatchedWallet(
                address=addr,
                chat_id=chat_id,
                note=note,
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
    elif skipped == 1 and added == 0 and _is_addr(first_addr):
        final_text = t(chat_id, "already_watching", addr=fmt_addr(first_addr))
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


def _render_list_page(
    chat_id: int,
    page: int,
    select_mode: bool = False,
) -> tuple[str, InlineKeyboardMarkup | None]:
    wallets = list(watched.get(chat_id, {}).items())
    total_pages = max(1, (len(wallets) + LIST_PAGE_SIZE - 1) // LIST_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * LIST_PAGE_SIZE
    chunk = wallets[start : start + LIST_PAGE_SIZE]

    selected = bulk_selection.get(chat_id, set())
    # Drop addresses that no longer exist (e.g. unwatched elsewhere) so the
    # selection count never goes stale.
    if selected:
        live_addrs = set(watched.get(chat_id, {}).keys())
        stale = selected - live_addrs
        if stale:
            selected -= stale
            bulk_selection[chat_id] = selected
    page_suffix = f":s" if select_mode else ""

    lines = [t(chat_id, "watch_list")]
    keyboard: list[list[InlineKeyboardButton]] = []
    for addr, w in chunk:
        flag = _wallet_badge(chat_id, w)
        note = f" · <b>{_html_escape(w.note)}</b>" if w.note else ""
        relt = _relative_time(chat_id, w.last_check)
        # In select mode, prefix each line with the checkbox state so the
        # text body itself shows what's currently selected (the buttons can
        # only fit one row per wallet).
        prefix = ""
        if select_mode:
            prefix = ("☑️ " if addr in selected else "☐ ")
        lines.append(
            f"{prefix}{flag} <code>{addr}</code>{note}\n"
            f"    {len(w.position_snapshot)} pos · {t(chat_id, 'label_last_check')}: {relt}"
        )
        label_title = (w.note or fmt_addr(addr))[:18]
        if select_mode:
            mark = "☑️" if addr in selected else "☐"
            keyboard.append(
                [
                    InlineKeyboardButton(
                        f"{mark} {label_title}",
                        callback_data=f"bulk:tog:{addr}:{page}",
                    ),
                ]
            )
        else:
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
            # Row 2: mute toggle · 💨 dust picker · unwatch (page-scoped so
            # the list returns to the same page after the action).
            keyboard.append(
                [
                    InlineKeyboardButton(
                        mute_label, callback_data=f"togglemute:{addr}:{page}"
                    ),
                    InlineKeyboardButton(
                        t(chat_id, "btn_dust"), callback_data=f"minfill:open:{addr}"
                    ),
                    InlineKeyboardButton(
                        t(chat_id, "btn_unwatch"),
                        callback_data=f"unwatch:{addr}:{page}",
                    ),
                ]
            )
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(
                InlineKeyboardButton(
                    "⬅",
                    callback_data=f"list_page:{page - 1}{page_suffix}",
                )
            )
        nav.append(
            InlineKeyboardButton(
                t(chat_id, "list_page_indicator", page=page + 1, total=total_pages),
                callback_data="noop",
            )
        )
        if page < total_pages - 1:
            nav.append(
                InlineKeyboardButton(
                    "➡",
                    callback_data=f"list_page:{page + 1}{page_suffix}",
                )
            )
        keyboard.append(nav)

    if select_mode:
        keyboard.append(
            [
                InlineKeyboardButton(
                    t(chat_id, "btn_bulk_all"),
                    callback_data=f"bulk:all:{page}",
                ),
                InlineKeyboardButton(
                    t(chat_id, "btn_bulk_clear"),
                    callback_data=f"bulk:clear:{page}",
                ),
                InlineKeyboardButton(
                    t(chat_id, "btn_bulk_exit"),
                    callback_data=f"bulk:exit:{page}",
                ),
            ]
        )
        keyboard.append(
            [
                InlineKeyboardButton(
                    t(chat_id, "btn_bulk_unwatch", n=len(selected)),
                    callback_data=f"bulk:del:{page}",
                ),
            ]
        )
    elif wallets:
        keyboard.append(
            [
                InlineKeyboardButton(
                    t(chat_id, "btn_bulk_enter"),
                    callback_data=f"bulk:enter:{page}",
                ),
                InlineKeyboardButton(
                    t(chat_id, "btn_copy_addrs"),
                    callback_data="listcopy",
                ),
            ]
        )

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
        await _prompt_addr_reply(ctx, chat_id, "pos")
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
        await _prompt_addr_reply(ctx, chat_id, "orders")
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
            [
                InlineKeyboardButton(
                    t(chat_id, "btn_chatdef_open"), callback_data="cdef:open"
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


async def cmd_defaults(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Open the chat-level micro-fill defaults picker."""
    chat_id = update.effective_chat.id
    await _show_chatdef_picker(ctx, chat_id)


async def cmd_export(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    wallets = watched.get(chat_id, {})
    fmt = (ctx.args[0].lower() if ctx.args else "json").strip()
    if fmt not in {"json", "txt", "csv"}:
        fmt = "json"

    if fmt == "txt":
        # One address per line — paste-friendly for other tools / scripts.
        body = "\n".join(addr for addr in wallets.keys())
        filename = "predict_watches.txt"
    elif fmt == "csv":
        # Quote notes through csv so commas / newlines in备注 don't break rows.
        out = StringIO()
        writer = csv.writer(out)
        writer.writerow(["address", "note", "muted", "positions", "last_check"])
        for a, w in wallets.items():
            writer.writerow(
                [a, w.note, int(bool(w.muted)), len(w.position_snapshot), int(w.last_check)]
            )
        body = out.getvalue()
        filename = "predict_watches.csv"
    else:
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
        body = json.dumps(payload, indent=2, ensure_ascii=False)
        filename = "predict_watches.json"

    buf = BytesIO(body.encode("utf-8"))
    buf.seek(0)
    await ctx.bot.send_document(
        chat_id=chat_id,
        document=InputFile(buf, filename=filename),
        caption=t(chat_id, "export_caption", count=len(wallets)),
    )


async def cmd_chatid(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Echo the caller's chat id. Handy for filling ADMIN_CHAT_ID on deploys."""
    chat_id = update.effective_chat.id
    user = update.effective_user
    uid = user.id if user else "?"
    await update.message.reply_text(
        f"chat_id: <code>{chat_id}</code>\nuser_id: <code>{uid}</code>",
        parse_mode="HTML",
    )


async def cmd_raw(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin-only: dump the raw matches + one position JSON for debugging.

    Usage: /raw 0xAddress [count]
    Gated by the ADMIN_CHAT_ID env var. Returns a .json file so nothing is
    clipped by Telegram's 4096-char message limit.
    """
    chat_id = update.effective_chat.id
    if ADMIN_CHAT_ID is None or chat_id != ADMIN_CHAT_ID:
        return
    if not ctx.args:
        await update.message.reply_text("Usage: /raw 0xAddress [count]")
        return
    addr = ctx.args[0].strip()
    if not (addr.startswith("0x") and len(addr) == 42):
        await update.message.reply_text("Invalid address")
        return
    try:
        count = int(ctx.args[1]) if len(ctx.args) > 1 else 10
    except ValueError:
        count = 10
    count = max(1, min(count, 50))

    async with aiohttp.ClientSession() as session:
        matches_url = f"{PREDICT_API}/v1/orders/matches"
        matches_params = {"signerAddress": addr, "first": str(count)}
        positions_url = f"{PREDICT_API}/v1/positions/{addr}"
        try:
            async with session.get(
                matches_url,
                params=matches_params,
                headers=_headers(),
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                matches_status = r.status
                matches_body = await r.text()
        except Exception as e:
            matches_status = 0
            matches_body = f"error: {e}"
        try:
            async with session.get(
                positions_url,
                headers=_headers(),
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                positions_status = r.status
                positions_body = await r.text()
        except Exception as e:
            positions_status = 0
            positions_body = f"error: {e}"

    def _parse(body: str):
        try:
            return json.loads(body)
        except Exception:
            return body

    payload = {
        "address": addr,
        "requested_at": int(time.time()),
        "matches": {
            "url": matches_url,
            "params": matches_params,
            "status": matches_status,
            "body": _parse(matches_body),
        },
        "positions": {
            "url": positions_url,
            "status": positions_status,
            "body": _parse(positions_body),
        },
    }
    buf = BytesIO(json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8"))
    buf.seek(0)
    await ctx.bot.send_document(
        chat_id=chat_id,
        document=InputFile(buf, filename=f"raw_{addr[:10]}.json"),
        caption=f"raw matches(first={count}) + positions for {addr[:10]}…{addr[-4:]}",
    )


async def cmd_apistatus(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin-only diagnostic: show recent Predict.fun API status counts and
    any wallets currently in rate-limit cooldown.

    Usage: /apistatus
    Gated by ADMIN_CHAT_ID like /raw. Aggregates the last 30 minutes of the
    rolling api_stats deque.
    """
    chat_id = update.effective_chat.id
    if ADMIN_CHAT_ID is None or chat_id != ADMIN_CHAT_ID:
        return

    now = time.time()
    window_s = 30 * 60
    labels = ("ok", "429", "5xx", "timeout", "other")

    lines = [f"API stats (last {window_s // 60} min, rolling)"]
    for endpoint in ("positions", "matches"):
        bucket = api_stats.get(endpoint) or ()
        counts = {lbl: 0 for lbl in labels}
        for ts, lbl in bucket:
            if now - ts <= window_s:
                counts[lbl] = counts.get(lbl, 0) + 1
        lines.append(
            f"{endpoint:<9} {counts['ok']} ok · {counts['429']} rate-limited · "
            f"{counts['5xx']} 5xx · {counts['timeout']} timeout · {counts['other']} other"
        )

    throttled = []
    for cid, wallets in watched.items():
        for addr_, w in wallets.items():
            if w.rate_limit_until and w.rate_limit_until > now:
                throttled.append((w.rate_limit_until - now, addr_, cid, w.rate_limit_level))
    throttled.sort()

    lines.append("")
    if throttled:
        lines.append(f"Currently throttled: {len(throttled)} wallets")
        for remaining, addr_, cid, level in throttled[:20]:
            lines.append(
                f"  • {addr_[:10]}…{addr_[-4:]} (chat {cid}) "
                f"— retry in {int(remaining)}s (level {level})"
            )
        if len(throttled) > 20:
            lines.append(f"  … and {len(throttled) - 20} more")
    else:
        lines.append("Currently throttled: none")

    body = "\n".join(lines)
    await update.message.reply_text(f"<pre>{body}</pre>", parse_mode="HTML")


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


async def cmd_dustinterval(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Set per-wallet dust-summary flush interval.

    Accepts an integer number of seconds, or a trailing unit: "5m" = 300s,
    "1h" = 3600s. Stored sentinels: 0 = fall back to global default, -1 =
    explicitly disabled (every poll). Users can also pass "off" / "关闭"
    to disable batching on a single wallet.
    """
    chat_id = update.effective_chat.id
    if len(ctx.args) < 2:
        await update.message.reply_text(t(chat_id, "usage_dustinterval"))
        return
    addr = resolve_addr(chat_id, ctx.args[0])
    target = watched.get(chat_id, {}).get(addr) if addr else None
    if not target:
        await update.message.reply_text(t(chat_id, "not_found"))
        return
    sec = _parse_dust_interval_raw(ctx.args[1])
    if sec is None:
        await update.message.reply_text(t(chat_id, "dustinterval_too_small"))
        return
    await _apply_dust_interval(ctx, chat_id, addr, target, sec)


async def _apply_dust_interval(
    ctx: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    addr: str,
    target: WatchedWallet,
    sec: int,
):
    """Persist a new dust interval and send the right confirmation message."""
    target.dust_interval_s = sec
    # Switching to 0/-1 lets the next poll drain; a new positive value
    # restarts the batch window so it honors the new duration.
    if sec > 0 and target.pending_dust_fills:
        target.last_dust_flush = time.time()
    save_watch(target)
    if sec == -1:
        reply = t(chat_id, "dustinterval_off", addr=fmt_addr(addr))
    elif sec == 0:
        reply = t(chat_id, "dustinterval_set_default", addr=fmt_addr(addr))
    else:
        reply = t(chat_id, "dustinterval_set", addr=fmt_addr(addr), sec=sec)
    await ctx.bot.send_message(chat_id=chat_id, text=reply, parse_mode="HTML")


MINFILL_PRESETS: tuple[float, ...] = (0.0, 0.5, 1.0, 5.0)
# Dust-interval presets, in seconds. -1 is the "every poll" sentinel; 0 is
# not a preset because it means "follow global default" (available via the
# dedicated Default button).
DUSTINTERVAL_PRESETS: tuple[int, ...] = (-1, 1800, 3600, 28800)


def _fmt_dust_interval(chat_id: int, sec: int) -> str:
    """Human-readable label for an effective dust-interval (used in the body).

    `sec == 0` here represents "every poll" (effective); callers that need to
    distinguish "follow global" should pass the resolved value in.
    """
    if sec <= 0:
        return t(chat_id, "dust_label_every_poll")
    if sec % 3600 == 0:
        return t(chat_id, "dust_label_hours", hours=sec // 3600)
    if sec % 60 == 0:
        return t(chat_id, "dust_label_minutes", mins=sec // 60)
    return t(chat_id, "dust_label_seconds", sec=sec)


def _effective_dust_interval(target: WatchedWallet) -> int:
    """Resolve the effective interval honoring -1/0/positive sentinels.

    Lookup order: wallet override (0 = inherit) → chat-level default
    (same -1/0/>0 sentinels) → env DUST_INTERVAL.
    """
    if target.dust_interval_s < 0:
        return 0
    if target.dust_interval_s > 0:
        return target.dust_interval_s
    cd = chat_defaults.get(target.chat_id, {})
    chat_val = int(cd.get("dust_interval_s", 0) or 0)
    if chat_val < 0:
        return 0
    if chat_val > 0:
        return chat_val
    return DUST_INTERVAL


def _effective_min_fill(target: WatchedWallet) -> float:
    """Resolve the effective dust floor.

    Lookup order: wallet override (>0 = use it) → chat-level default
    (>0 = use it) → env MIN_FILL_USD.
    """
    if target.min_fill_usd > 0:
        return target.min_fill_usd
    cd = chat_defaults.get(target.chat_id, {})
    chat_val = float(cd.get("min_fill_usd", 0) or 0)
    if chat_val > 0:
        return chat_val
    return MIN_FILL_USD


def _minfill_body(chat_id: int, addr: str, target: WatchedWallet) -> str:
    """Picker body: shows both dust floor and dust-summary interval."""
    floor = _effective_min_fill(target)
    floor_fallback = (
        t(chat_id, "minfill_picker_fallback")
        if target.min_fill_usd == 0 and floor > 0
        else ""
    )
    interval_sec = _effective_dust_interval(target)
    interval_label = _fmt_dust_interval(chat_id, interval_sec)
    interval_fallback = (
        t(chat_id, "minfill_picker_fallback") if target.dust_interval_s == 0 else ""
    )
    return "\n".join(
        [
            t(chat_id, "minfill_picker_title", addr=fmt_addr(addr)),
            "",
            t(chat_id, "minfill_picker_floor", usd=floor, fallback=floor_fallback),
            t(
                chat_id,
                "minfill_picker_interval",
                interval=interval_label,
                fallback=interval_fallback,
            ),
            "",
            t(chat_id, "minfill_picker_hint"),
        ]
    )


def _dust_preset_label(chat_id: int, sec: int) -> str:
    """Short label for an interval preset button — keeps buttons compact."""
    if sec < 0:
        return t(chat_id, "dustinterval_preset_every_poll")
    if sec % 3600 == 0:
        return t(chat_id, "dust_btn_hours", hours=sec // 3600)
    if sec % 60 == 0:
        return t(chat_id, "dust_btn_minutes", mins=sec // 60)
    return t(chat_id, "dust_btn_seconds", sec=sec)


def _minfill_keyboard(chat_id: int, addr: str, target: WatchedWallet) -> InlineKeyboardMarkup:
    # Row 1 — dust-floor presets (💰).
    current = target.min_fill_usd
    floor_row = []
    for usd in MINFILL_PRESETS:
        if usd == 0:
            label = t(chat_id, "minfill_preset_off")
        else:
            label = f"${usd:g}"
        if abs(current - usd) < 1e-6:
            label = f"✅ {label}"
        floor_row.append(
            InlineKeyboardButton(label, callback_data=f"minfill:set:{addr}:{usd:g}")
        )
    floor_row.append(
        InlineKeyboardButton(
            t(chat_id, "btn_minfill_custom"),
            callback_data=f"minfill:custom:{addr}",
        )
    )

    # Row 2 — dust-interval presets (⏱).
    interval_row = []
    for sec in DUSTINTERVAL_PRESETS:
        label = _dust_preset_label(chat_id, sec)
        if sec == target.dust_interval_s:
            label = f"✅ {label}"
        interval_row.append(
            InlineKeyboardButton(label, callback_data=f"minfill:dset:{addr}:{sec}")
        )
    interval_row.append(
        InlineKeyboardButton(
            t(chat_id, "btn_dustinterval_custom"),
            callback_data=f"minfill:dcustom:{addr}",
        )
    )

    # Row 3 — follow-global + back.
    default_label = t(chat_id, "btn_dustinterval_default")
    if target.dust_interval_s == 0:
        default_label = f"✅ {default_label}"
    bottom_row = [
        InlineKeyboardButton(default_label, callback_data=f"minfill:ddefault:{addr}"),
        InlineKeyboardButton(
            t(chat_id, "btn_back"), callback_data=f"minfill:close:{addr}"
        ),
    ]
    return InlineKeyboardMarkup([floor_row, interval_row, bottom_row])


def _chatdef_body(chat_id: int) -> str:
    """Body for the chat-level defaults picker."""
    cd = chat_defaults.get(chat_id, {})
    floor_raw = float(cd.get("min_fill_usd", 0) or 0)
    dust_raw = int(cd.get("dust_interval_s", 0) or 0)
    floor = floor_raw if floor_raw > 0 else MIN_FILL_USD
    floor_fallback = (
        t(chat_id, "chatdef_picker_fallback") if floor_raw == 0 and floor > 0 else ""
    )
    if dust_raw < 0:
        interval_sec = 0
    elif dust_raw > 0:
        interval_sec = dust_raw
    else:
        interval_sec = DUST_INTERVAL
    interval_label = _fmt_dust_interval(chat_id, interval_sec)
    interval_fallback = (
        t(chat_id, "chatdef_picker_fallback") if dust_raw == 0 else ""
    )
    return "\n".join(
        [
            t(chat_id, "chatdef_picker_title"),
            "",
            t(chat_id, "chatdef_picker_floor", usd=floor, fallback=floor_fallback),
            t(
                chat_id,
                "chatdef_picker_interval",
                interval=interval_label,
                fallback=interval_fallback,
            ),
            "",
            t(chat_id, "chatdef_picker_hint"),
        ]
    )


def _chatdef_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    cd = chat_defaults.get(chat_id, {})
    floor_raw = float(cd.get("min_fill_usd", 0) or 0)
    dust_raw = int(cd.get("dust_interval_s", 0) or 0)

    # Row 1 — floor presets.
    floor_row = []
    for usd in MINFILL_PRESETS:
        label = (
            t(chat_id, "minfill_preset_off") if usd == 0 else f"${usd:g}"
        )
        if abs(floor_raw - usd) < 1e-6:
            label = f"✅ {label}"
        floor_row.append(
            InlineKeyboardButton(label, callback_data=f"cdef:set:{usd:g}")
        )
    floor_row.append(
        InlineKeyboardButton(
            t(chat_id, "btn_minfill_custom"),
            callback_data="cdef:custom",
        )
    )

    # Row 2 — interval presets.
    interval_row = []
    for sec in DUSTINTERVAL_PRESETS:
        label = _dust_preset_label(chat_id, sec)
        if sec == dust_raw:
            label = f"✅ {label}"
        interval_row.append(
            InlineKeyboardButton(label, callback_data=f"cdef:dset:{sec}")
        )
    interval_row.append(
        InlineKeyboardButton(
            t(chat_id, "btn_dustinterval_custom"),
            callback_data="cdef:dcustom",
        )
    )

    # Row 3 — back.
    return InlineKeyboardMarkup(
        [
            floor_row,
            interval_row,
            [InlineKeyboardButton(t(chat_id, "btn_back"), callback_data="cdef:close")],
        ]
    )


async def _show_chatdef_picker(
    ctx: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    *,
    query=None,
):
    body = _chatdef_body(chat_id)
    markup = _chatdef_keyboard(chat_id)
    if query is not None:
        try:
            await query.edit_message_text(body, parse_mode="HTML", reply_markup=markup)
            return
        except BadRequest:
            pass
    await ctx.bot.send_message(
        chat_id=chat_id, text=body, parse_mode="HTML", reply_markup=markup
    )


async def _show_minfill_picker(
    update: Update | None,
    ctx: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    addr: str,
    *,
    query=None,
):
    target = watched.get(chat_id, {}).get(addr)
    if not target:
        if query is not None:
            try:
                await query.edit_message_text(t(chat_id, "not_found"))
            except BadRequest:
                pass
        elif update is not None and update.message is not None:
            await update.message.reply_text(t(chat_id, "not_found"))
        return
    body = _minfill_body(chat_id, addr, target)
    markup = _minfill_keyboard(chat_id, addr, target)
    if query is not None:
        try:
            await query.edit_message_text(
                body, parse_mode="HTML", reply_markup=markup
            )
            return
        except BadRequest:
            pass
    if update is not None and update.message is not None:
        await update.message.reply_text(
            body, parse_mode="HTML", reply_markup=markup
        )
    else:
        await ctx.bot.send_message(
            chat_id=chat_id, text=body, parse_mode="HTML", reply_markup=markup
        )


def _set_pending_minfill(ctx: ContextTypes.DEFAULT_TYPE, chat_id: int, addr: str):
    ctx.user_data["pending_minfill"] = {"chat_id": chat_id, "address": addr}


def _pop_pending_minfill(ctx: ContextTypes.DEFAULT_TYPE) -> dict | None:
    return ctx.user_data.pop("pending_minfill", None)


def _set_pending_dustinterval(ctx: ContextTypes.DEFAULT_TYPE, chat_id: int, addr: str):
    ctx.user_data["pending_dustinterval"] = {"chat_id": chat_id, "address": addr}


def _pop_pending_dustinterval(ctx: ContextTypes.DEFAULT_TYPE) -> dict | None:
    return ctx.user_data.pop("pending_dustinterval", None)


def _set_pending_chatdef_floor(ctx: ContextTypes.DEFAULT_TYPE, chat_id: int):
    ctx.user_data["pending_chatdef_floor"] = {"chat_id": chat_id}


def _pop_pending_chatdef_floor(ctx: ContextTypes.DEFAULT_TYPE) -> dict | None:
    return ctx.user_data.pop("pending_chatdef_floor", None)


def _set_pending_chatdef_interval(ctx: ContextTypes.DEFAULT_TYPE, chat_id: int):
    ctx.user_data["pending_chatdef_interval"] = {"chat_id": chat_id}


def _pop_pending_chatdef_interval(ctx: ContextTypes.DEFAULT_TYPE) -> dict | None:
    return ctx.user_data.pop("pending_chatdef_interval", None)


def _parse_dust_interval_raw(raw: str) -> int | None:
    """Parse a user-supplied interval string into a stored sentinel value.

    Returns the integer seconds to persist (-1 = off, 0 = follow global,
    positive = custom). Returns None when the input is unparseable or the
    value is out of range (nonzero positive below 30s).
    """
    raw = raw.strip().lower().lstrip("=")
    if not raw:
        return None
    if raw in ("off", "disable", "disabled", "关闭", "每次"):
        return -1
    if raw in ("default", "global", "默认", "全局"):
        return 0
    mult = 1
    if raw.endswith("m"):
        raw, mult = raw[:-1], 60
    elif raw.endswith("h"):
        raw, mult = raw[:-1], 3600
    elif raw.endswith("s"):
        raw = raw[:-1]
    try:
        sec = int(raw) * mult
    except ValueError:
        return None
    if sec < -1:
        return None
    if sec > 0 and sec < 30:
        return None
    return sec


async def cmd_minfill(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if len(ctx.args) < 1:
        await update.message.reply_text(t(chat_id, "usage_minfill"))
        return
    addr = resolve_addr(chat_id, ctx.args[0])
    target = watched.get(chat_id, {}).get(addr) if addr else None
    if not target:
        await update.message.reply_text(t(chat_id, "not_found"))
        return
    # /minfill <addr> — open picker.
    if len(ctx.args) == 1:
        await _show_minfill_picker(update, ctx, chat_id, addr)
        return
    try:
        usd = float(ctx.args[1])
    except ValueError:
        await update.message.reply_text(t(chat_id, "usage_minfill"))
        return
    if usd < 0:
        await update.message.reply_text(t(chat_id, "minfill_bad_amount"))
        return
    target.min_fill_usd = usd
    save_watch(target)
    await update.message.reply_text(
        t(chat_id, "minfill_set", addr=fmt_addr(addr), usd=usd),
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
        await _prompt_addr_reply(ctx, chat_id, "portfolio")
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
            if isinstance(positions, RateLimited):
                positions = None
            if isinstance(matches, RateLimited):
                matches = None
            if positions is None and matches is None:
                skipped += 1
                continue
            positions = positions or []
            matches = matches or []
            snapshot = {pos_key(p): pos_size(p) for p in positions}
            title_cache = {pos_key(p): _title_cache_entry(p) for p in positions}
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


def _set_pending_watch(ctx: ContextTypes.DEFAULT_TYPE, chat_id: int):
    ctx.user_data["pending_watch"] = {"chat_id": chat_id}


def _pop_pending_watch(ctx: ContextTypes.DEFAULT_TYPE) -> dict | None:
    return ctx.user_data.pop("pending_watch", None)


async def on_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle free-text replies to bot prompts (note-edit / watch-address)."""
    if not update.message or update.message.text is None:
        return
    chat_id = update.effective_chat.id

    # Reply-to-watch flow: user tapped the "➕ Watch wallet" button or sent
    # /watch with no args, then replied to our ForceReply prompt with an
    # address.
    pending_watch = ctx.user_data.get("pending_watch")
    if pending_watch and pending_watch.get("chat_id") == chat_id:
        tokens = [tok for tok in update.message.text.split() if tok.strip()]
        first = tokens[0] if tokens else ""
        if not (first.startswith("0x") and len(first) == 42):
            # Keep the pending state so the next reply is still captured.
            await update.message.reply_text(
                t(chat_id, "watch_reply_invalid"),
                parse_mode="HTML",
                reply_markup=ForceReply(selective=True),
            )
            return
        _pop_pending_watch(ctx)
        ctx.args = tokens
        await cmd_watch(update, ctx)
        return

    # Reply-to-query flow: /pos, /orders, /portfolio without args sends a
    # ForceReply prompt; the next text reply is routed back into the same
    # handler with ctx.args populated.
    pending_query = ctx.user_data.get("pending_addr_query")
    if pending_query and pending_query.get("chat_id") == chat_id:
        kind = pending_query.get("kind")
        token = (update.message.text or "").strip().split(maxsplit=1)
        first = token[0] if token else ""
        # Accept either a raw 0x address or a known alias / note from the
        # watch list — resolve_addr handles both transparently.
        addr = resolve_addr(chat_id, first) if first else None
        if not addr:
            await update.message.reply_text(
                t(chat_id, "watch_reply_invalid"),
                parse_mode="HTML",
                reply_markup=ForceReply(selective=True),
            )
            return
        ctx.user_data.pop("pending_addr_query", None)
        ctx.args = [addr]
        if kind == "orders":
            await cmd_orders(update, ctx)
        elif kind == "portfolio":
            await cmd_portfolio(update, ctx)
        else:
            await cmd_pos(update, ctx)
        return

    # Dust-floor custom-amount reply.
    pending_minfill = ctx.user_data.get("pending_minfill")
    if pending_minfill and pending_minfill.get("chat_id") == chat_id:
        addr = pending_minfill.get("address") or ""
        target = watched.get(chat_id, {}).get(addr)
        if not target:
            _pop_pending_minfill(ctx)
            return
        raw = (update.message.text or "").strip().lstrip("$").replace(",", "")
        try:
            usd = float(raw)
        except ValueError:
            await update.message.reply_text(
                t(chat_id, "minfill_bad_number"),
                reply_markup=ForceReply(selective=True),
            )
            return
        if usd < 0:
            await update.message.reply_text(
                t(chat_id, "minfill_bad_number"),
                reply_markup=ForceReply(selective=True),
            )
            return
        _pop_pending_minfill(ctx)
        target.min_fill_usd = usd
        save_watch(target)
        await update.message.reply_text(
            t(chat_id, "minfill_set", addr=fmt_addr(addr), usd=usd),
            parse_mode="HTML",
        )
        return

    # Dust-interval custom reply (from the picker's ✏️ button).
    pending_dust = ctx.user_data.get("pending_dustinterval")
    if pending_dust and pending_dust.get("chat_id") == chat_id:
        addr = pending_dust.get("address") or ""
        target = watched.get(chat_id, {}).get(addr)
        if not target:
            _pop_pending_dustinterval(ctx)
            return
        sec = _parse_dust_interval_raw(update.message.text or "")
        if sec is None:
            await update.message.reply_text(
                t(chat_id, "dustinterval_bad_input"),
                reply_markup=ForceReply(selective=True),
            )
            return
        _pop_pending_dustinterval(ctx)
        await _apply_dust_interval(ctx, chat_id, addr, target, sec)
        return

    # Chat-default floor custom reply.
    pending_cdef_floor = ctx.user_data.get("pending_chatdef_floor")
    if pending_cdef_floor and pending_cdef_floor.get("chat_id") == chat_id:
        raw = (update.message.text or "").strip().lstrip("$").replace(",", "")
        try:
            usd = float(raw)
        except ValueError:
            await update.message.reply_text(
                t(chat_id, "minfill_bad_number"),
                reply_markup=ForceReply(selective=True),
            )
            return
        if usd < 0:
            await update.message.reply_text(
                t(chat_id, "minfill_bad_number"),
                reply_markup=ForceReply(selective=True),
            )
            return
        _pop_pending_chatdef_floor(ctx)
        cd = chat_defaults.setdefault(chat_id, {"min_fill_usd": 0.0, "dust_interval_s": 0})
        cd["min_fill_usd"] = usd
        save_chat_defaults(chat_id)
        await _show_chatdef_picker(ctx, chat_id)
        return

    # Chat-default interval custom reply.
    pending_cdef_int = ctx.user_data.get("pending_chatdef_interval")
    if pending_cdef_int and pending_cdef_int.get("chat_id") == chat_id:
        sec = _parse_dust_interval_raw(update.message.text or "")
        if sec is None:
            await update.message.reply_text(
                t(chat_id, "dustinterval_bad_input"),
                reply_markup=ForceReply(selective=True),
            )
            return
        _pop_pending_chatdef_interval(ctx)
        cd = chat_defaults.setdefault(chat_id, {"min_fill_usd": 0.0, "dust_interval_s": 0})
        cd["dust_interval_s"] = sec
        save_chat_defaults(chat_id)
        await _show_chatdef_picker(ctx, chat_id)
        return

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
        # unwatch:<addr>[:<page>] — keep the user on the same page after the
        # row disappears instead of jumping back to page 1.
        parts = data.split(":")
        addr = parts[1].strip() if len(parts) > 1 else ""
        try:
            page = int(parts[2]) if len(parts) > 2 else 0
        except ValueError:
            page = 0
        if addr in watched.get(chat_id, {}):
            del watched[chat_id][addr]
            delete_watch(chat_id, addr)
            bulk_selection.get(chat_id, set()).discard(addr)
        # Refresh the list in place; clamping in _render_list_page handles
        # the "removed last item on the last page" case.
        if watched.get(chat_id):
            text, markup = _render_list_page(chat_id, page)
            try:
                await query.edit_message_text(text, parse_mode="HTML", reply_markup=markup)
            except BadRequest:
                pass
        else:
            bulk_selection.pop(chat_id, None)
            try:
                await query.edit_message_text(t(chat_id, "no_watched_wallets"))
            except BadRequest:
                pass
        return

    if data.startswith("togglemute:"):
        parts = data.split(":")
        addr = parts[1].strip() if len(parts) > 1 else ""
        try:
            page = int(parts[2]) if len(parts) > 2 else 0
        except ValueError:
            page = 0
        target = watched.get(chat_id, {}).get(addr)
        if target:
            target.muted = not target.muted
            save_watch(target)
            text, markup = _render_list_page(chat_id, page)
            try:
                await query.edit_message_text(text, parse_mode="HTML", reply_markup=markup)
            except BadRequest:
                pass
        return

    if data.startswith("list_page:"):
        # list_page:<page>[:s] — :s suffix preserves multi-select mode while
        # paging.
        parts = data.split(":")
        try:
            page = int(parts[1])
        except (ValueError, IndexError):
            return
        select_mode = len(parts) > 2 and parts[2] == "s"
        text, markup = _render_list_page(chat_id, page, select_mode=select_mode)
        try:
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=markup)
        except BadRequest:
            pass
        return

    if data == "listcopy":
        addrs = list(watched.get(chat_id, {}).keys())
        if not addrs:
            try:
                await query.answer(t(chat_id, "no_watched_wallets"), show_alert=False)
            except Exception:
                pass
            return
        try:
            await query.answer()
        except Exception:
            pass
        joined = "\n".join(addrs)
        # <pre> renders as a tap-to-copy block on Telegram clients. Stay clear
        # of the 4096-char message ceiling (43 chars per address × ~95 fits
        # comfortably; anything bigger goes out as a .txt attachment).
        if len(joined) <= 3800:
            await ctx.bot.send_message(
                chat_id=chat_id,
                text=f"<pre>{_html_escape(joined)}</pre>",
                parse_mode="HTML",
            )
        else:
            buf = BytesIO(joined.encode("utf-8"))
            buf.seek(0)
            await ctx.bot.send_document(
                chat_id=chat_id,
                document=InputFile(buf, filename="predict_addresses.txt"),
                caption=t(chat_id, "copy_addrs_caption", count=len(addrs)),
            )
        return

    if data.startswith("bulk:"):
        parts = data.split(":")
        action = parts[1] if len(parts) > 1 else ""

        def _page_arg(idx: int) -> int:
            try:
                return int(parts[idx])
            except (ValueError, IndexError):
                return 0

        if action == "enter":
            page = _page_arg(2)
            bulk_selection.setdefault(chat_id, set())
            text, markup = _render_list_page(chat_id, page, select_mode=True)
            try:
                await query.edit_message_text(text, parse_mode="HTML", reply_markup=markup)
            except BadRequest:
                pass
            return

        if action == "exit":
            page = _page_arg(2)
            bulk_selection.pop(chat_id, None)
            text, markup = _render_list_page(chat_id, page, select_mode=False)
            try:
                await query.edit_message_text(text, parse_mode="HTML", reply_markup=markup)
            except BadRequest:
                pass
            return

        if action == "tog":
            addr = parts[2] if len(parts) > 2 else ""
            page = _page_arg(3)
            if addr in watched.get(chat_id, {}):
                sel = bulk_selection.setdefault(chat_id, set())
                if addr in sel:
                    sel.discard(addr)
                else:
                    sel.add(addr)
            text, markup = _render_list_page(chat_id, page, select_mode=True)
            try:
                await query.edit_message_text(text, parse_mode="HTML", reply_markup=markup)
            except BadRequest:
                pass
            return

        if action == "all":
            page = _page_arg(2)
            wallets = list(watched.get(chat_id, {}).items())
            start = page * LIST_PAGE_SIZE
            chunk = wallets[start : start + LIST_PAGE_SIZE]
            sel = bulk_selection.setdefault(chat_id, set())
            sel.update(addr for addr, _ in chunk)
            text, markup = _render_list_page(chat_id, page, select_mode=True)
            try:
                await query.edit_message_text(text, parse_mode="HTML", reply_markup=markup)
            except BadRequest:
                pass
            return

        if action == "clear":
            page = _page_arg(2)
            bulk_selection[chat_id] = set()
            text, markup = _render_list_page(chat_id, page, select_mode=True)
            try:
                await query.edit_message_text(text, parse_mode="HTML", reply_markup=markup)
            except BadRequest:
                pass
            return

        if action == "del":
            page = _page_arg(2)
            sel = bulk_selection.get(chat_id, set())
            if not sel:
                try:
                    await query.answer(
                        t(chat_id, "bulk_no_selection"), show_alert=False
                    )
                except Exception:
                    pass
                return
            removed = 0
            for addr in list(sel):
                if addr in watched.get(chat_id, {}):
                    del watched[chat_id][addr]
                    delete_watch(chat_id, addr)
                    removed += 1
            bulk_selection.pop(chat_id, None)
            try:
                await query.answer(
                    t(chat_id, "bulk_unwatch_done", count=removed),
                    show_alert=False,
                )
            except Exception:
                pass
            if watched.get(chat_id):
                text, markup = _render_list_page(chat_id, page, select_mode=False)
                try:
                    await query.edit_message_text(
                        text, parse_mode="HTML", reply_markup=markup
                    )
                except BadRequest:
                    pass
            else:
                try:
                    await query.edit_message_text(t(chat_id, "no_watched_wallets"))
                except BadRequest:
                    pass
            return

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

    if data == "watch_prompt":
        await _prompt_watch_reply(ctx, chat_id)
        return

    # Dust picker: minfill:{open|set|custom|close|dset|dcustom|ddefault}:{addr}[:{value}]
    # (the prefix is "minfill" for legacy reasons; the picker now covers both
    # the dust floor and the dust-summary interval)
    if data.startswith("minfill:"):
        parts = data.split(":", 3)
        action = parts[1] if len(parts) > 1 else ""
        addr = parts[2] if len(parts) > 2 else ""
        target = watched.get(chat_id, {}).get(addr)
        if not target:
            try:
                await query.edit_message_text(t(chat_id, "not_found"))
            except BadRequest:
                pass
            return
        if action == "open":
            await _show_minfill_picker(None, ctx, chat_id, addr, query=query)
            return
        if action == "set" and len(parts) >= 4:
            try:
                usd = float(parts[3])
            except ValueError:
                return
            if usd < 0:
                return
            target.min_fill_usd = usd
            save_watch(target)
            await _show_minfill_picker(None, ctx, chat_id, addr, query=query)
            return
        if action == "custom":
            _set_pending_minfill(ctx, chat_id, addr)
            await ctx.bot.send_message(
                chat_id=chat_id,
                text=t(chat_id, "minfill_custom_prompt"),
                parse_mode="HTML",
                reply_markup=ForceReply(selective=True),
            )
            return
        if action == "dset" and len(parts) >= 4:
            try:
                sec = int(parts[3])
            except ValueError:
                return
            if sec < -1 or (sec > 0 and sec < 30):
                return
            prev = target.dust_interval_s
            target.dust_interval_s = sec
            # Restart the batch window if we just switched to a new positive
            # interval while dust was buffered; switching to 0/-1 lets the
            # next poll drain naturally.
            if sec > 0 and target.pending_dust_fills and prev != sec:
                target.last_dust_flush = time.time()
            save_watch(target)
            await _show_minfill_picker(None, ctx, chat_id, addr, query=query)
            return
        if action == "ddefault":
            target.dust_interval_s = 0
            save_watch(target)
            await _show_minfill_picker(None, ctx, chat_id, addr, query=query)
            return
        if action == "dcustom":
            _set_pending_dustinterval(ctx, chat_id, addr)
            await ctx.bot.send_message(
                chat_id=chat_id,
                text=t(chat_id, "dustinterval_custom_prompt"),
                parse_mode="HTML",
                reply_markup=ForceReply(selective=True),
            )
            return
        if action == "close":
            try:
                await query.edit_message_text(
                    t(
                        chat_id,
                        "minfill_set",
                        addr=fmt_addr(addr),
                        usd=_effective_min_fill(target),
                    ),
                    parse_mode="HTML",
                )
            except BadRequest:
                pass
            return
        return

    # Chat-level micro-fill defaults picker: cdef:{open|set|custom|close|dset|dcustom}[:{value}]
    if data.startswith("cdef:"):
        parts = data.split(":", 2)
        action = parts[1] if len(parts) > 1 else ""
        payload = parts[2] if len(parts) > 2 else ""
        cd = chat_defaults.setdefault(chat_id, {"min_fill_usd": 0.0, "dust_interval_s": 0})
        if action == "open":
            await _show_chatdef_picker(ctx, chat_id, query=query)
            return
        if action == "set":
            try:
                usd = float(payload)
            except ValueError:
                return
            if usd < 0:
                return
            cd["min_fill_usd"] = usd
            save_chat_defaults(chat_id)
            await _show_chatdef_picker(ctx, chat_id, query=query)
            return
        if action == "dset":
            try:
                sec = int(payload)
            except ValueError:
                return
            if sec < -1 or (sec > 0 and sec < 30):
                return
            cd["dust_interval_s"] = sec
            save_chat_defaults(chat_id)
            await _show_chatdef_picker(ctx, chat_id, query=query)
            return
        if action == "custom":
            _set_pending_chatdef_floor(ctx, chat_id)
            await ctx.bot.send_message(
                chat_id=chat_id,
                text=t(chat_id, "minfill_custom_prompt"),
                parse_mode="HTML",
                reply_markup=ForceReply(selective=True),
            )
            return
        if action == "dcustom":
            _set_pending_chatdef_interval(ctx, chat_id)
            await ctx.bot.send_message(
                chat_id=chat_id,
                text=t(chat_id, "dustinterval_custom_prompt"),
                parse_mode="HTML",
                reply_markup=ForceReply(selective=True),
            )
            return
        if action == "close":
            try:
                await query.edit_message_text(
                    t(chat_id, "chatdef_saved"),
                    parse_mode="HTML",
                )
            except BadRequest:
                pass
            return
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
        # predict.fun flips `status` to "closed" when trading ends, which can
        # happen hours or days before the winning outcome is recorded. Firing
        # a 市场已结算 alert at that point produces "获胜结果: ?" and, worse,
        # stamps resolved_markets so the real resolution never alerts. Wait
        # until a winning index is actually present.
        if winning is None:
            continue
        prior = w.resolved_markets.get(str(mid))
        # Re-alert if an older build stamped a stub entry with winning=None
        # before this guard existed — otherwise those wallets would silently
        # miss the real resolution.
        if prior and prior.get("winning") is not None:
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
    sem = asyncio.Semaphore(POLL_CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        while True:
            cycle_started = time.monotonic()

            async def _poll_one(chat_id, addr, w):
                async with sem:
                    try:
                        # The outer iteration uses a list() snapshot, so a
                        # /unwatch (or bulk delete) that lands between two
                        # polls would otherwise leave the snapshotted entry
                        # alive long enough for save_watch() to resurrect
                        # it in the DB — and a container restart would then
                        # reload the "deleted" wallet on load_state. Bail
                        # immediately if the user already removed it.
                        if addr not in watched.get(chat_id, {}):
                            return
                        # Per-wallet interval gating: skip wallets whose
                        # personal interval hasn't elapsed. 0 = use global.
                        if (
                            w.poll_interval_s
                            and (time.time() - w.last_check) < w.poll_interval_s
                        ):
                            return

                        # Rate-limit cooldown gate: if the API recently 429'd
                        # for this wallet, skip both calls until the backoff
                        # window expires. The window is set when fetch_*
                        # returns a RateLimited sentinel below.
                        if w.rate_limit_until and time.time() < w.rate_limit_until:
                            return

                        positions_raw = await fetch_positions(session, w.address)
                        matches_raw = await fetch_order_matches(session, w.address, first=20)

                        # 429 handling: extract Retry-After (preferring the
                        # larger of the two endpoints' hints), apply
                        # exponential backoff, and emit a one-shot Telegram
                        # alert once we've been throttled twice in a row.
                        rl_results = [
                            r for r in (positions_raw, matches_raw)
                            if isinstance(r, RateLimited)
                        ]
                        if rl_results:
                            retry_after = max(
                                (r.retry_after_s or 0) for r in rl_results
                            )
                            w.rate_limit_level += 1
                            backoff = (
                                retry_after
                                if retry_after > 0
                                else min(30 * (2 ** (w.rate_limit_level - 1)), 300)
                            )
                            w.rate_limit_until = time.time() + backoff
                            if (
                                w.rate_limit_level >= 2
                                and not w.rate_limit_notified
                                and not w.muted
                            ):
                                try:
                                    await app.bot.send_message(
                                        chat_id=chat_id,
                                        text=t(
                                            chat_id,
                                            "rate_limited_alert",
                                            addr=fmt_addr(w.address),
                                            backoff=int(backoff),
                                        ),
                                        parse_mode="HTML",
                                    )
                                    w.rate_limit_notified = True
                                except Exception as send_err:
                                    logger.warning(
                                        f"Failed to send rate_limited_alert for {addr}: {send_err}"
                                    )
                            # Coerce RateLimited → None so downstream
                            # diff/error logic treats this as a failed fetch
                            # without trying to iterate the sentinel.
                            if isinstance(positions_raw, RateLimited):
                                positions_raw = None
                            if isinstance(matches_raw, RateLimited):
                                matches_raw = None

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
                                return

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

                            # Clear rate-limit cooldown after a clean poll.
                            if w.rate_limit_level or w.rate_limit_until:
                                rl_was_notified = w.rate_limit_notified
                                w.rate_limit_level = 0
                                w.rate_limit_until = 0
                                w.rate_limit_notified = False
                                if rl_was_notified and not w.muted:
                                    try:
                                        await app.bot.send_message(
                                            chat_id=chat_id,
                                            text=t(
                                                chat_id,
                                                "rate_limit_recovered",
                                                addr=fmt_addr(w.address),
                                            ),
                                            parse_mode="HTML",
                                        )
                                    except Exception as send_err:
                                        logger.warning(
                                            f"Failed to send rate_limit_recovered for {addr}: {send_err}"
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
                        for m in matches:
                            mid = _match_market_view(m).get("id")
                            if mid:
                                market_ids.add(mid)
                        markets = {mid: await fetch_market(session, mid) for mid in market_ids}
                        for p in positions:
                            p["_market"] = markets.get(p.get("marketId"), {})
                        for m in matches:
                            view = _match_market_view(m)
                            fresh = markets.get(view.get("id")) or {}
                            # Fold both the flat match fields and the cached
                            # /v1/markets/{id} payload into match["market"] so
                            # downstream helpers (title, slug, URL) see the
                            # richest possible dict regardless of which API
                            # shape the upstream returned.
                            merged = {**view, **fresh}
                            # Don't let the generic category title from
                            # /v1/markets/{id} overwrite a specific per-trade
                            # market_title the orders API gave us.
                            if view.get("title"):
                                merged["title"] = view["title"]
                            if view.get("slug"):
                                merged["slug"] = view["slug"]
                            if merged:
                                m["market"] = merged
                        positions_by_key = {pos_key(p): p for p in positions}

                        if positions_ok:
                            added, changed, closed = diff_positions(
                                w.position_snapshot,
                                positions,
                                threshold_pct=w.change_threshold_pct,
                            )
                        else:
                            added, changed, closed = [], [], []

                        dust_fills: list[dict] = []
                        if matches_ok:
                            new_match_keys = {match_key(m) for m in matches}
                            new_fills = [
                                m for m in matches
                                if match_key(m) not in w.order_match_snapshot
                            ]
                            # Dust filter: fills whose USD notional is below
                            # the effective floor fold into a single summary
                            # line instead of firing full cards. Lookup order
                            # is wallet → chat default → env fallback, via
                            # _effective_min_fill.
                            effective_min = _effective_min_fill(w)
                            if effective_min > 0 and new_fills:
                                kept: list[dict] = []
                                for m in new_fills:
                                    v = _fill_usd_value(m, w.address)
                                    if v is not None and v < effective_min:
                                        dust_fills.append(m)
                                    else:
                                        kept.append(m)
                                new_fills = kept
                        else:
                            new_match_keys = None
                            new_fills = []

                        # Markets that have a brand-new fill will already be
                        # fully described by the 订单成交 block (which now
                        # shows total holding + delta + PnL). Suppress the
                        # duplicate 持仓变化 entry for the same market+outcome
                        # to avoid two near-identical messages in a row.
                        # Dust fills count too: their position delta is tiny
                        # and already represented by the summary line.
                        fill_market_keys: set[str] = set()
                        for m in list(new_fills) + list(dust_fills):
                            k = _fill_market_outcome_key(m)
                            if k:
                                fill_market_keys.add(k)

                        # Refresh the shares snapshot + the title cache. Hold onto
                        # old titles so "closed" notifications can show a real
                        # market name even though the position is gone now.
                        new_titles: dict[str, dict] = {
                            pos_key(p): _title_cache_entry(p, p.get("_market"))
                            for p in positions
                        }
                        old_titles = dict(w.position_titles)
                        # Preserve titles for keys that have disappeared this
                        # poll so we can still render their resolution notice.
                        merged_titles = {**old_titles, **new_titles}
                        if positions_ok:
                            w.position_titles = new_titles
                            w.position_snapshot = {pos_key(p): pos_size(p) for p in positions}
                        w.last_check = time.time()
                        display_addr = w.address
                        display_note = f" · {_html_escape(w.note)}" if w.note else ""

                        # --- Detect market resolutions (fires once per market) ---
                        resolutions = _detect_resolutions(
                            w, markets, merged_titles, positions_by_key
                        )

                        blocks: list[str] = []  # For merged combined message.

                        changed_visible = [
                            (p, prev) for (p, prev) in changed
                            if pos_key(p) not in fill_market_keys
                        ]
                        added_visible = [
                            p for p in added
                            if pos_key(p) not in fill_market_keys
                        ]
                        # Map market+outcome → most recent fill so 新开仓 /
                        # 持仓变化 cards can surface the on-chain tx hash even
                        # when the fill itself wasn't emitted separately (e.g.
                        # suppressed as dust or already seen in a prior poll).
                        # ``matches`` arrives newest-first from the API, so the
                        # first entry per key is the freshest.
                        matches_by_key: dict[str, dict] = {}
                        for m in matches:
                            k = _fill_market_outcome_key(m)
                            if k and k not in matches_by_key:
                                matches_by_key[k] = m

                        if added_visible or changed_visible or closed:
                            parts = []
                            for p in added_visible:
                                parts.append(
                                    fmt_pos(
                                        p,
                                        "added",
                                        chat_id,
                                        markets.get(p.get("marketId")),
                                        match=matches_by_key.get(pos_key(p)),
                                    )
                                )
                            for p, prev_size in changed_visible:
                                parts.append(
                                    fmt_pos(
                                        p,
                                        "changed",
                                        chat_id,
                                        markets.get(p.get("marketId")),
                                        prev_shares=prev_size,
                                        match=matches_by_key.get(pos_key(p)),
                                    )
                                )
                            for k in closed:
                                entry = merged_titles.get(k)
                                if isinstance(entry, dict):
                                    title = entry.get("title") or t(
                                        chat_id, "close_fallback", key=k
                                    )
                                    outcome = entry.get("outcome") or ""
                                    slug = entry.get("slug")
                                    market_stub = (
                                        {"slug": slug} if slug else None
                                    )
                                    title_html = _title_html(title, market_stub)
                                else:
                                    # Legacy cache entry (plain title string).
                                    title_html = _html_escape(
                                        entry
                                        or t(chat_id, "close_fallback", key=k)
                                    )
                                    outcome = ""
                                closed_line = (
                                    f"{t(chat_id, 'fmt_closed')} <b>{title_html}</b>"
                                )
                                if outcome:
                                    closed_line += (
                                        f"\n✅ <b>{_html_escape(outcome)}</b>"
                                    )
                                parts.append(closed_line)
                            block = t(chat_id, "poll_header", addr=display_addr, note=display_note) + "\n\n".join(parts[:8])
                            if len(parts) > 8:
                                block += t(chat_id, "fmt_more", count=len(parts) - 8)
                            blocks.append(block)

                        # Dust-summary batching: when the effective dust
                        # interval is >0, defer this poll's dust fills into
                        # w.pending_dust_fills and only flush after the
                        # interval has elapsed since the batch window
                        # started. Effective value of 0 = flush every poll.
                        # Resolution (see _effective_dust_interval): wallet
                        # override → chat default → env DUST_INTERVAL.
                        # last_dust_flush==0 means "no active batch window";
                        # it's stamped when the first dust of a batch
                        # arrives and reset on flush.
                        effective_dust_interval = _effective_dust_interval(w)
                        dust_to_emit: list[dict] = []
                        if dust_fills:
                            if effective_dust_interval > 0:
                                if not w.pending_dust_fills:
                                    w.last_dust_flush = time.time()
                                w.pending_dust_fills.extend(dust_fills)
                            else:
                                dust_to_emit.extend(dust_fills)
                        if w.pending_dust_fills:
                            if effective_dust_interval == 0:
                                # Batching turned off while buffer was full —
                                # drain immediately.
                                dust_to_emit.extend(w.pending_dust_fills)
                                w.pending_dust_fills = []
                                w.last_dust_flush = 0
                            elif (time.time() - w.last_dust_flush) >= effective_dust_interval:
                                dust_to_emit.extend(w.pending_dust_fills)
                                w.pending_dust_fills = []
                                w.last_dust_flush = 0

                        if new_fills or dust_to_emit:
                            merged_fills = consolidate_fills(new_fills) if new_fills else []
                            fill_lines = [
                                fmt_match(m, chat_id, positions_by_key, address=w.address)
                                for m in merged_fills[:5]
                            ]
                            if dust_to_emit:
                                dust_total = sum(
                                    (v for m in dust_to_emit
                                     if (v := _fill_usd_value(m, w.address)) is not None),
                                    0.0,
                                )
                                fill_lines.append(
                                    t(
                                        chat_id,
                                        "dust_summary",
                                        count=len(dust_to_emit),
                                        usd=dust_total,
                                    )
                                )
                            blocks.append(
                                t(chat_id, "fills_header", addr=display_addr, note=display_note)
                                + "\n\n".join(fill_lines)
                            )

                        if resolutions:
                            lines = []
                            for market, winning_idx, held_idx, title in resolutions:
                                lines.append(
                                    fmt_resolution(chat_id, market, winning_idx, held_idx)
                                )
                            blocks.append(
                                t(chat_id, "poll_resolve_header", addr=display_addr, note=display_note)
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
                        # Re-check after all the awaits — a delete that
                        # arrived while we were talking to predict.fun
                        # must not be overwritten by this snapshot.
                        if addr in watched.get(chat_id, {}):
                            save_watch(w)
                    except Exception as e:
                        logger.error(f"Poll error {addr}: {e}")

            tasks = [
                _poll_one(chat_id, addr, w)
                for chat_id, wallets in list(watched.items())
                for addr, w in list(wallets.items())
            ]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            elapsed = time.monotonic() - cycle_started
            await asyncio.sleep(max(0.0, POLL_INTERVAL - elapsed))

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
            BotCommand("settings", "偏好设置 / Settings"),
            BotCommand("defaults", "小额默认 / Micro-fill defaults"),
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
    app.add_handler(CommandHandler("note", cmd_note))
    app.add_handler(CommandHandler("mute", cmd_mute))
    app.add_handler(CommandHandler("unmute", cmd_unmute))
    app.add_handler(CommandHandler("settings", cmd_settings))
    app.add_handler(CommandHandler("defaults", cmd_defaults))
    app.add_handler(CommandHandler("export", cmd_export))
    app.add_handler(CommandHandler("import", cmd_import))
    app.add_handler(CommandHandler("raw", cmd_raw))
    app.add_handler(CommandHandler("apistatus", cmd_apistatus))
    app.add_handler(CommandHandler("chatid", cmd_chatid))
    app.add_handler(CommandHandler("threshold", cmd_threshold))
    app.add_handler(CommandHandler("interval", cmd_interval))
    app.add_handler(CommandHandler("dustinterval", cmd_dustinterval))
    app.add_handler(CommandHandler("minfill", cmd_minfill))
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
