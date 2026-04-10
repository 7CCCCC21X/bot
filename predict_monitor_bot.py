# “””
Predict.fun 钱包监控 Telegram Bot

Input EOA address -> poll GET /v1/positions/{address} -> push changes to Telegram

Deps: pip install python-telegram-bot aiohttp

Config:

1. TELEGRAM_BOT_TOKEN - from @BotFather
1. PREDICT_API_KEY - from Predict Discord
1. python predict_monitor_bot.py
   “””

import asyncio
import logging
import time
import json
import hashlib
from dataclasses import dataclass, field

import aiohttp
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ==================== 配置 ====================

import os
TELEGRAM_BOT_TOKEN = os.environ.get(“TELEGRAM_BOT_TOKEN”, “”)
PREDICT_API_KEY = os.environ.get(“PREDICT_API_KEY”, “”)
PREDICT_API = “https://api.predict.fun”
POLL_INTERVAL = 15  # 秒

logging.basicConfig(format=”%(asctime)s [%(levelname)s] %(message)s”, level=logging.INFO)
logger = logging.getLogger(**name**)

# ==================== 数据结构 ====================

@dataclass
class WatchedWallet:
address: str
chat_id: int
position_snapshot: dict = field(default_factory=dict)  # key -> hash
last_check: float = 0

# chat_id -> {address -> WatchedWallet}

watched: dict[int, dict[str, WatchedWallet]] = {}

# market 缓存

market_cache: dict[str, dict] = {}

# ==================== API ====================

def _headers():
return {“Content-Type”: “application/json”, “x-api-key”: PREDICT_API_KEY}

async def fetch_positions(session: aiohttp.ClientSession, address: str) -> list[dict]:
“”“GET /v1/positions/{address} — 支持 EOA 地址直接查询”””
url = f”{PREDICT_API}/v1/positions/{address}”
try:
async with session.get(url, headers=_headers(), timeout=aiohttp.ClientTimeout(total=15)) as resp:
if resp.status == 200:
data = await resp.json()
return data.get(“data”, []) if data.get(“success”) else []
logger.warning(f”Positions API {resp.status} for {address}”)
return []
except Exception as e:
logger.error(f”fetch_positions error: {e}”)
return []

async def fetch_market(session: aiohttp.ClientSession, market_id: str) -> dict:
if market_id in market_cache:
return market_cache[market_id]
url = f”{PREDICT_API}/v1/markets/{market_id}”
try:
async with session.get(url, headers=_headers(), timeout=aiohttp.ClientTimeout(total=10)) as resp:
if resp.status == 200:
data = await resp.json()
if data.get(“success”):
market_cache[market_id] = data.get(“data”, {})
return market_cache[market_id]
except Exception:
pass
return {}

# ==================== 变化检测 ====================

def pos_key(pos: dict) -> str:
return f”{pos.get(‘marketId’, ‘’)}_{pos.get(‘outcomeIndex’, ‘’)}”

def pos_hash(pos: dict) -> str:
fields = {
“shares”: str(pos.get(“shares”, “”)),
“avgPrice”: str(pos.get(“avgPrice”, “”)),
}
return hashlib.md5(json.dumps(fields, sort_keys=True).encode()).hexdigest()

def diff_positions(old: dict, new_list: list[dict]):
“”“返回 (新增, 变化, 关闭)”””
new_map = {pos_key(p): pos_hash(p) for p in new_list}
new_by_key = {pos_key(p): p for p in new_list}

```
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
```

# ==================== 格式化 ====================

def fmt_addr(addr: str) -> str:
return f”{addr[:6]}…{addr[-4:]}”

def fmt_pos(pos: dict, label: str) -> str:
title = (pos.get(“title”) or pos.get(“marketId”, “?”))[:55]
outcome = pos.get(“outcomeName”) or pos.get(“outcomeIndex”, “?”)
shares = pos.get(“shares”, “?”)
avg = pos.get(“avgPrice”, “?”)
try:
val = f”${float(shares) * float(avg):,.2f}”
except (ValueError, TypeError):
val = “N/A”

```
emoji = {"added": "🟢 新仓位", "changed": "🔄 仓位变动", "closed": "🔴 平仓"}.get(label, "📊")
return f"{emoji}\n📊 {title}\n🎯 {outcome}\n📦 {shares} 股 × {avg}¢ = {val}"
```

def fmt_summary(positions: list[dict]) -> str:
if not positions:
return “📭 无持仓”
total = 0
lines = []
for p in positions[:20]:
title = (p.get(“title”) or p.get(“marketId”, “?”))[:40]
outcome = p.get(“outcomeName”) or str(p.get(“outcomeIndex”, “?”))
shares = p.get(“shares”, “0”)
price = p.get(“currentPrice”) or p.get(“avgPrice”, “?”)
try:
v = float(shares) * float(price)
total += v
lines.append(f”• {outcome} | {shares}股 @ {price}¢ = ${v:,.2f}\n  {title}”)
except (ValueError, TypeError):
lines.append(f”• {outcome} | {shares}股 @ {price}¢\n  {title}”)

```
return f"💼 {len(positions)} 个持仓 | 估值 ${total:,.2f}\n\n" + "\n\n".join(lines)
```

# ==================== Telegram 命令 ====================

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
await update.message.reply_text(
“🔮 <b>Predict.fun 监控 Bot</b>\n\n”
“/watch <code>0x地址</code> — 监控钱包 (EOA)\n”
“/unwatch <code>0x地址</code> — 取消\n”
“/list — 监控列表\n”
“/pos <code>0x地址</code> — 查看持仓\n”
“/stop — 全部停止”,
parse_mode=“HTML”,
)

async def cmd_watch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
chat_id = update.effective_chat.id
if not ctx.args:
return await update.message.reply_text(“用法: /watch 0x地址”)

```
addr = ctx.args[0].strip()
if not addr.startswith("0x") or len(addr) != 42:
    return await update.message.reply_text("❌ 无效地址")

if chat_id not in watched:
    watched[chat_id] = {}
if addr.lower() in {a.lower() for a in watched[chat_id]}:
    return await update.message.reply_text(f"⚠️ 已在监控 <code>{fmt_addr(addr)}</code>", parse_mode="HTML")

await update.message.reply_text("⏳ 获取初始持仓...")

async with aiohttp.ClientSession() as session:
    positions = await fetch_positions(session, addr)

snapshot = {pos_key(p): pos_hash(p) for p in positions}
watched[chat_id][addr] = WatchedWallet(
    address=addr, chat_id=chat_id,
    position_snapshot=snapshot, last_check=time.time(),
)

await update.message.reply_text(
    f"✅ 开始监控 <code>{fmt_addr(addr)}</code>\n"
    f"📊 当前持仓: {len(positions)} 个\n"
    f"⏱ 间隔: {POLL_INTERVAL}s",
    parse_mode="HTML",
)
```

async def cmd_unwatch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
chat_id = update.effective_chat.id
if not ctx.args:
return await update.message.reply_text(“用法: /unwatch 0x地址”)
addr = ctx.args[0].strip()
to_remove = None
for a in watched.get(chat_id, {}):
if a.lower() == addr.lower():
to_remove = a
break
if to_remove:
del watched[chat_id][to_remove]
await update.message.reply_text(f”✅ 已取消 <code>{fmt_addr(addr)}</code>”, parse_mode=“HTML”)
else:
await update.message.reply_text(“❌ 未找到”)

async def cmd_list(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
wallets = watched.get(update.effective_chat.id, {})
if not wallets:
return await update.message.reply_text(“📭 无监控”)
lines = [“📋 <b>监控列表</b>\n”]
for addr, w in wallets.items():
lines.append(f”• <code>{fmt_addr(addr)}</code>  ({len(w.position_snapshot)} 仓位)”)
await update.message.reply_text(”\n”.join(lines), parse_mode=“HTML”)

async def cmd_pos(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
if not ctx.args:
return await update.message.reply_text(“用法: /pos 0x地址”)
addr = ctx.args[0].strip()
async with aiohttp.ClientSession() as session:
positions = await fetch_positions(session, addr)
text = f”📊 <code>{fmt_addr(addr)}</code>\n\n{fmt_summary(positions)}”
await update.message.reply_text(text, parse_mode=“HTML”)

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
n = len(watched.pop(update.effective_chat.id, {}))
await update.message.reply_text(f”🛑 已停止 {n} 个监控”)

# ==================== 后台轮询 ====================

async def poll_loop(app: Application):
await asyncio.sleep(3)
logger.info(“Poll loop started”)

```
async with aiohttp.ClientSession() as session:
    while True:
        for chat_id, wallets in list(watched.items()):
            for addr, w in list(wallets.items()):
                try:
                    positions = await fetch_positions(session, w.address)
                    added, changed, closed = diff_positions(w.position_snapshot, positions)

                    # 更新快照
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
                        parts.append(f"🔴 平仓: {k}")

                    header = f"🔔 <b>Predict.fun</b> — <code>{fmt_addr(w.address)}</code>\n\n"
                    text = header + "\n\n".join(parts[:8])
                    if len(parts) > 8:
                        text += f"\n\n... 还有 {len(parts) - 8} 条"

                    await app.bot.send_message(
                        chat_id=chat_id, text=text,
                        parse_mode="HTML", disable_web_page_preview=True,
                    )
                except Exception as e:
                    logger.error(f"Poll error {addr}: {e}")

                await asyncio.sleep(1)

        await asyncio.sleep(POLL_INTERVAL)
```

# ==================== 启动 ====================

def main():
app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
app.add_handler(CommandHandler(“start”, cmd_start))
app.add_handler(CommandHandler(“watch”, cmd_watch))
app.add_handler(CommandHandler(“unwatch”, cmd_unwatch))
app.add_handler(CommandHandler(“list”, cmd_list))
app.add_handler(CommandHandler(“pos”, cmd_pos))
app.add_handler(CommandHandler(“positions”, cmd_pos))
app.add_handler(CommandHandler(“stop”, cmd_stop))

```
loop = asyncio.get_event_loop()
app.post_init = lambda _app: loop.create_task(poll_loop(_app))

logger.info("Bot starting...")
app.run_polling()
```

if **name** == “**main**”:
main()
