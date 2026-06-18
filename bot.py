"""
Grow a Garden — Stock Predictor Bot
A public-ready Discord bot that notifies users before items restock on
luminon.top/gag2. Uses slash commands, autocomplete, SQLite persistence,
and a shared HTTP cache so a single bot instance can serve many users
across many servers efficiently.

Setup:
  1. pip install -r requirements.txt
  2. export DISCORD_TOKEN="your_bot_token"
  3. (optional) export DEV_GUILD_ID="123..." for instant command sync
  4. python bot.py

Bot permissions needed: Send Messages, Embed Links, Use Slash Commands.
Privileged intents: none (Message Content NOT required).
"""

from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()

import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Optional

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks

# ─── CONFIG ───────────────────────────────────────────────────────────────────

TOKEN          = os.environ.get("DISCORD_TOKEN")
raw_guilds = os.environ.get("DEV_GUILD_ID")
DEV_GUILD_IDS = [int(g.strip()) for g in raw_guilds.split(",") if g.strip().isdigit()] if raw_guilds else []
DB_PATH        = Path(os.environ.get("BOT_DB_PATH", "gagbot.db"))
SITE_URL       = "https://luminon.top/gag2/"

FETCH_INTERVAL = 240   # seconds — refresh site data this often
NOTIFY_TICK    = 20    # seconds — how often we check pending notifications
MAX_WATCHES    = 25    # per user
MIN_NOTIFY_MIN = 1
MAX_NOTIFY_MIN = 60
HTTP_TIMEOUT   = 15

# Categories the bot supports
CATEGORIES = ("seeds", "gears", "weather")

# Which weather events are worth notifying for (others are too frequent)
WEATHER_EVENTS = ("Goldmoon", "Rainbow Moon", "Bloodmoon")

# Rarity → embed colour
RARITY_COLORS = {
    "Common":    0x9A9A9F,
    "Uncommon":  0x28C828,
    "Rare":      0x3C82F0,
    "Epic":      0xAA50E6,
    "Legendary": 0xE0A800,
    "Mythic":    0xDC3232,
    "Super":     0xFF2A2A,
    "Divine":    0xCFCFCF,
}

CATEGORY_EMOJI = {"seeds": "🌱", "gears": "⚙️", "weather": "🌙"}
BRAND_COLOR    = 0x8F21D1
SUCCESS_COLOR  = 0x5DFF52
ERROR_COLOR    = 0xDC3232
INFO_COLOR     = 0x3C82F0

# ─── LOGGING ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("gagbot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("gagbot")
logging.getLogger("discord").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)


# ─── FORMATTING HELPERS ───────────────────────────────────────────────────────

def fmt_dur(seconds: int) -> str:
    """Format a duration in seconds as e.g. '2h 5m', '12m', '45s'."""
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}s"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{m}m {s}s" if s else f"{m}m"
    h, mm = divmod(m, 60)
    return f"{h}h {mm}m" if mm else f"{h}h"


def fmt_price(n: int) -> str:
    """Compact currency formatting: 1500 -> '1.5K', 12000000 -> '12.0M'."""
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


# ─── DATABASE ─────────────────────────────────────────────────────────────────
# We use stdlib sqlite3 inside a tiny lock to keep things simple. The hot path
# (notification loop) does a few small reads/writes; this is fine for thousands
# of users without resorting to async DB libraries.

_DB_LOCK = asyncio.Lock()
_db_conn: Optional[sqlite3.Connection] = None


def db_init() -> None:
    """Create the database schema if it doesn't exist."""
    global _db_conn
    _db_conn = sqlite3.connect(DB_PATH, isolation_level=None, check_same_thread=False)
    _db_conn.row_factory = sqlite3.Row
    _db_conn.executescript("""
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;

        CREATE TABLE IF NOT EXISTS watches (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id         INTEGER NOT NULL,
            channel_id      INTEGER,                    -- NULL = DM
            guild_id        INTEGER,                    -- NULL = DM
            category        TEXT    NOT NULL,           -- 'seeds' | 'gears' | 'weather'
            item_name       TEXT    NOT NULL,
            notify_minutes  INTEGER NOT NULL,
            created_at      INTEGER NOT NULL,
            UNIQUE(user_id, category, item_name)        -- one watch per item per user
        );
        CREATE INDEX IF NOT EXISTS idx_watches_user ON watches(user_id);

        CREATE TABLE IF NOT EXISTS notifications_sent (
            user_id     INTEGER NOT NULL,
            category    TEXT    NOT NULL,
            item_name   TEXT    NOT NULL,
            restock_ts  INTEGER NOT NULL,
            PRIMARY KEY (user_id, category, item_name, restock_ts)
        );
    """)
    log.info("Database ready at %s", DB_PATH)


@contextmanager
def db_cursor():
    """Context manager yielding a cursor on the shared connection."""
    assert _db_conn is not None, "db_init() must be called first"
    cur = _db_conn.cursor()
    try:
        yield cur
    finally:
        cur.close()


async def db_execute(sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    """Run a query under the DB lock and return all rows."""
    async with _DB_LOCK:
        with db_cursor() as cur:
            cur.execute(sql, params)
            try:
                return cur.fetchall()
            except sqlite3.ProgrammingError:
                return []


# DAL — small helpers wrapping the queries we actually use.

async def watch_add(user_id: int, channel_id: Optional[int], guild_id: Optional[int],
                    category: str, item_name: str, minutes: int) -> str:
    """Insert or replace a watch; returns 'added' or 'updated'."""
    existing = await db_execute(
        "SELECT id FROM watches WHERE user_id=? AND category=? AND item_name=?",
        (user_id, category, item_name),
    )
    if existing:
        await db_execute(
            "UPDATE watches SET channel_id=?, guild_id=?, notify_minutes=? WHERE id=?",
            (channel_id, guild_id, minutes, existing[0]["id"]),
        )
        return "updated"
    await db_execute(
        "INSERT INTO watches(user_id, channel_id, guild_id, category, item_name, "
        "notify_minutes, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (user_id, channel_id, guild_id, category, item_name, minutes, int(time.time())),
    )
    return "added"


async def watch_remove(user_id: int, category: str, item_name: str) -> bool:
    """Returns True if a row was deleted."""
    async with _DB_LOCK:
        with db_cursor() as cur:
            cur.execute(
                "DELETE FROM watches WHERE user_id=? AND category=? AND item_name=?",
                (user_id, category, item_name),
            )
            return cur.rowcount > 0


async def watch_count(user_id: int) -> int:
    rows = await db_execute("SELECT COUNT(*) AS c FROM watches WHERE user_id=?", (user_id,))
    return rows[0]["c"] if rows else 0


async def watch_list_for(user_id: int) -> list[sqlite3.Row]:
    return await db_execute(
        "SELECT * FROM watches WHERE user_id=? ORDER BY category, item_name",
        (user_id,),
    )


async def watch_clear(user_id: int) -> int:
    async with _DB_LOCK:
        with db_cursor() as cur:
            cur.execute("DELETE FROM watches WHERE user_id=?", (user_id,))
            return cur.rowcount


async def watch_all() -> list[sqlite3.Row]:
    return await db_execute("SELECT * FROM watches")


async def notif_seen(user_id: int, category: str, item_name: str, restock_ts: int) -> bool:
    rows = await db_execute(
        "SELECT 1 FROM notifications_sent WHERE user_id=? AND category=? "
        "AND item_name=? AND restock_ts=?",
        (user_id, category, item_name, restock_ts),
    )
    return bool(rows)


async def notif_mark(user_id: int, category: str, item_name: str, restock_ts: int) -> None:
    await db_execute(
        "INSERT OR IGNORE INTO notifications_sent(user_id, category, item_name, restock_ts) "
        "VALUES(?, ?, ?, ?)",
        (user_id, category, item_name, restock_ts),
    )


async def notif_cleanup() -> None:
    """Drop notification records older than 1h to keep the table small."""
    cutoff = int(time.time()) - 3600
    await db_execute("DELETE FROM notifications_sent WHERE restock_ts < ?", (cutoff,))


# ─── SITE SCRAPER + SHARED CACHE ──────────────────────────────────────────────

class DataCache:
    """
    Holds the latest snapshot of the site's DATA object plus a precomputed
    list of item names per category for fast autocomplete.
    """
    __slots__ = ("data", "fetched_at", "names", "_session")

    def __init__(self) -> None:
        self.data: Optional[dict] = None
        self.fetched_at: float = 0.0
        self.names: dict[str, list[str]] = {"seeds": [], "gears": [], "weather": list(WEATHER_EVENTS)}
        self._session: Optional[aiohttp.ClientSession] = None

    async def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
                headers={"User-Agent": "Mozilla/5.0 GAG-StockBot/2.0"},
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    @staticmethod
    def _extract_data_object(html: str) -> Optional[dict]:
        """
        The site embeds a giant `let DATA = {...};` literal in a <script> tag.
        We find it and walk braces (respecting string literals + escapes) to
        find the matching close brace — that's faster and safer than regex on
        a ~5 MB string.
        """
        marker = "let DATA = "
        start = html.find(marker)
        if start == -1:
            return None
        start += len(marker)

        depth = 0
        in_string = False
        escape = False
        for i in range(start, len(html)):
            c = html[i]
            if escape:
                escape = False
                continue
            if c == "\\" and in_string:
                escape = True
                continue
            if c == '"':
                in_string = not in_string
                continue
            if in_string:
                continue
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(html[start : i + 1])
                    except json.JSONDecodeError as e:
                        log.error("Failed to parse DATA object: %s", e)
                        return None
        return None

    async def fetch(self) -> bool:
        """Pull the page and update self.data. Returns True on success."""
        try:
            sess = await self.session()
            async with sess.get(SITE_URL) as resp:
                resp.raise_for_status()
                html = await resp.text()
        except Exception as e:
            log.warning("Site fetch failed: %s", e)
            return False

        data = self._extract_data_object(html)
        if not data:
            log.warning("DATA object not found in HTML")
            return False

        self.data = data
        self.fetched_at = time.time()
        self.names["seeds"]   = [i["name"] for i in data.get("seeds", [])]
        self.names["gears"]   = [i["name"] for i in data.get("gears", [])]
        # weather names stay constant (WEATHER_EVENTS)
        log.info("Data refreshed (%d seeds, %d gears)",
                 len(self.names["seeds"]), len(self.names["gears"]))
        return True

    async def ensure_fresh(self) -> Optional[dict]:
        """Return cached data, refreshing if stale or missing."""
        if self.data is None or time.time() - self.fetched_at > FETCH_INTERVAL:
            await self.fetch()
        return self.data


CACHE = DataCache()


# ─── PREDICTION LOGIC ─────────────────────────────────────────────────────────
# Direct port of the JavaScript on the site. Both seeds and gears use a
# precomputed quantity-per-slot array `q[]`; weather uses a phase schedule.

def get_next_restock(data: dict, category: str, item_name: str,
                     now_ts: Optional[int] = None) -> Optional[dict]:
    """
    For seeds/gears: find the next slot where q[k] > 0.
    Returns {'secs': until_restock, 'qty': qty_in_stock, 'restock_ts': unix_ts}
    or None if the item never restocks within the predicted window.
    """
    if now_ts is None:
        now_ts = int(time.time())

    period = data.get("period", 300)
    anchor = data.get("gearAnchor" if category == "gears" else "seedAnchor", 0)
    items  = data.get(category, [])
    item   = next((it for it in items if it["name"] == item_name), None)
    if not item:
        return None

    q = item.get("q", [])
    count = data.get("count", len(q))
    idx = max(0, (now_ts - anchor) // period)

    for k in range(idx + 1, count):
        if k < len(q) and q[k] > 0:
            t = anchor + k * period
            return {"secs": t - now_ts, "qty": q[k], "restock_ts": t}
    return None


def get_next_weather(data: dict, event_name: str,
                     now_ts: Optional[int] = None) -> Optional[dict]:
    """
    Walk the weather phase schedule forward until we find the next occurrence
    of `event_name`. Returns {'secs', 'restock_ts'} or None.
    """
    if now_ts is None:
        now_ts = int(time.time())

    W = data.get("weather", {})
    clen   = W.get("clen")
    phases = W.get("phases", [])
    seq    = W.get("seq", [])
    start  = W.get("startCycle", 0)
    if not (clen and phases and seq):
        return None

    cyc = now_ts // clen
    into = now_ts - cyc * clen
    pi = 0
    for i, p in enumerate(phases):
        if into >= p["offset"]:
            pi = i

    # Walk forward up to len(seq) * len(phases) steps (well-bounded)
    steps = len(seq) * len(phases)
    for _ in range(steps):
        pi += 1
        if pi >= len(phases):
            pi = 0
            cyc += 1
        idx = cyc - start
        if idx < 0 or idx >= len(seq):
            continue
        if seq[idx][pi] == event_name:
            start_t = cyc * clen + phases[pi]["offset"]
            return {"secs": start_t - now_ts, "restock_ts": start_t}
    return None


def get_next(data: dict, category: str, item_name: str,
             now_ts: Optional[int] = None) -> Optional[dict]:
    """Dispatch by category."""
    if category == "weather":
        return get_next_weather(data, item_name, now_ts)
    return get_next_restock(data, category, item_name, now_ts)


def find_item_meta(data: dict, category: str, item_name: str) -> dict:
    """Return rarity/price metadata for an item (empty dict if not applicable)."""
    if category == "weather":
        return {"rarity": "Event", "price": 0}
    item = next((it for it in data.get(category, []) if it["name"] == item_name), None)
    return item or {}


# ─── DISCORD BOT ──────────────────────────────────────────────────────────────

class GAGBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        # We intentionally do NOT request message_content — pure slash commands.
        super().__init__(command_prefix="!gag-unused-", intents=intents, help_command=None)

    async def setup_hook(self) -> None:
        # Boot the cache so autocomplete works from the first interaction.
        await CACHE.fetch()

        # Sync slash commands. If DEV_GUILD_ID is set, copy globals to that guild
        # for instant updates (global sync can take up to 1h to propagate).
        if DEV_GUILD_IDS:
            for g_id in DEV_GUILD_IDS:
                guild = discord.Object(id=g_id)
                self.tree.copy_global_to(guild=guild)
                synced = await self.tree.sync(guild=guild)
                log.info("Synced %d commands to dev guild %s", len(synced), g_id)
        else:
            synced = await self.tree.sync()
            log.info("Synced %d global commands", len(synced))

        # Start the background notification loop.
        notification_loop.start()



    async def close(self) -> None:
        await CACHE.close()
        await super().close()


bot = GAGBot()


@bot.event
async def on_ready() -> None:
    log.info("Logged in as %s (id=%s) — in %d guilds",
             bot.user, bot.user.id, len(bot.guilds))
    await bot.change_presence(activity=discord.Game(name="/help · Grow a Garden stock"))


# ─── SHARED EMBED HELPERS ─────────────────────────────────────────────────────

def make_embed(title: str, description: str = "", color: int = INFO_COLOR) -> discord.Embed:
    embed = discord.Embed(title=title, description=description, color=color)
    embed.set_footer(text="GaG Stock Bot · data: luminon.top")
    return embed


def error_embed(message: str) -> discord.Embed:
    return make_embed("Error", message, ERROR_COLOR)


# ─── AUTOCOMPLETE ─────────────────────────────────────────────────────────────

async def item_autocomplete(interaction: discord.Interaction, current: str
                            ) -> list[app_commands.Choice[str]]:
    """Suggest item names matching `current` based on the chosen category."""
    # The 'category' param is filled before 'item' by Discord clients
    category = None
    for opt in interaction.data.get("options", []):
        # walk the options tree (subcommand groups would nest, but we don't use them)
        if opt.get("name") == "category":
            category = opt.get("value")
            break
        for sub in opt.get("options", []) or []:
            if sub.get("name") == "category":
                category = sub.get("value")
                break

    if category not in CACHE.names:
        return []

    q = (current or "").lower()
    names = CACHE.names[category]
    # Rank: prefix matches first, then substring matches, then the rest
    starts, contains = [], []
    for n in names:
        nl = n.lower()
        if nl.startswith(q):
            starts.append(n)
        elif q in nl:
            contains.append(n)
    suggestions = (starts + contains)[:25] if q else names[:25]
    return [app_commands.Choice(name=n, value=n) for n in suggestions]


CATEGORY_CHOICES = [
    app_commands.Choice(name="🌱 Seeds",   value="seeds"),
    app_commands.Choice(name="⚙️ Gears",   value="gears"),
    app_commands.Choice(name="🌙 Weather", value="weather"),
]


# ─── SLASH COMMANDS ───────────────────────────────────────────────────────────

@bot.tree.command(name="watch", description="Get notified before an item restocks")
@app_commands.describe(
    category="Seeds, Gears, or Weather event",
    item="Item or event name",
    minutes=f"How many minutes before restock to notify you ({MIN_NOTIFY_MIN}–{MAX_NOTIFY_MIN})",
)
@app_commands.choices(category=CATEGORY_CHOICES)
@app_commands.autocomplete(item=item_autocomplete)
async def watch(interaction: discord.Interaction,
                category: app_commands.Choice[str],
                item: str,
                minutes: app_commands.Range[int, MIN_NOTIFY_MIN, MAX_NOTIFY_MIN] = 5) -> None:
    await interaction.response.defer(ephemeral=True)

    cat = category.value

    # Validate item name against the live list
    valid_names = CACHE.names.get(cat, [])
    match = next((n for n in valid_names if n.lower() == item.lower()), None)
    if not match:
        # try fuzzy
        match = next((n for n in valid_names if item.lower() in n.lower()), None)
    if not match:
        await interaction.followup.send(
            embed=error_embed(f"Unknown {cat[:-1]} `{item}`. Try `/items category:{cat}`."),
            ephemeral=True,
        )
        return

    # Enforce per-user limit
    if (await watch_count(interaction.user.id)) >= MAX_WATCHES:
        # Allow updates to existing watches, just not new ones beyond the cap
        existing = await db_execute(
            "SELECT id FROM watches WHERE user_id=? AND category=? AND item_name=?",
            (interaction.user.id, cat, match),
        )
        if not existing:
            await interaction.followup.send(
                embed=error_embed(
                    f"You've reached the limit of **{MAX_WATCHES}** watches. "
                    "Remove some with `/unwatch` or `/clear`."
                ),
                ephemeral=True,
            )
            return

    # Persist the watch — DMs send channel_id as None
    in_dm = interaction.guild is None
    action = await watch_add(
        user_id    = interaction.user.id,
        channel_id = None if in_dm else interaction.channel_id,
        guild_id   = None if in_dm else interaction.guild_id,
        category   = cat,
        item_name  = match,
        minutes    = minutes,
    )

    # Build a friendly confirmation
    data = await CACHE.ensure_fresh()
    eta = ""
    if data:
        nxt = get_next(data, cat, match)
        if nxt:
            extra = f" (qty x{nxt['qty']})" if cat != "weather" else ""
            eta = f"\n**Next restock:** in {fmt_dur(nxt['secs'])}{extra}"
        else:
            eta = "\n*No upcoming restock in the prediction window.*"

    meta = find_item_meta(data or {}, cat, match)
    color = RARITY_COLORS.get(meta.get("rarity", ""), SUCCESS_COLOR)
    destination = "DMs" if in_dm else f"<#{interaction.channel_id}>"
    verb = "Updated" if action == "updated" else "Added"

    embed = make_embed(
        f"✅ {verb} watch",
        f"{CATEGORY_EMOJI[cat]} **{match}** · *{cat}*\n"
        f"You'll be pinged in {destination} **{minutes} minute(s)** before restock."
        f"{eta}",
        color,
    )
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="unwatch", description="Stop watching an item")
@app_commands.describe(category="Seeds, Gears, or Weather", item="Item to remove")
@app_commands.choices(category=CATEGORY_CHOICES)
@app_commands.autocomplete(item=item_autocomplete)
async def unwatch(interaction: discord.Interaction,
                  category: app_commands.Choice[str],
                  item: str) -> None:
    await interaction.response.defer(ephemeral=True)

    cat = category.value
    valid_names = CACHE.names.get(cat, [])
    match = next((n for n in valid_names if n.lower() == item.lower()), None) or item

    removed = await watch_remove(interaction.user.id, cat, match)
    if removed:
        await interaction.followup.send(
            embed=make_embed("✅ Watch removed",
                             f"{CATEGORY_EMOJI[cat]} **{match}** · *{cat}*",
                             SUCCESS_COLOR),
            ephemeral=True,
        )
    else:
        await interaction.followup.send(
            embed=error_embed(f"You weren't watching `{match}` in *{cat}*."),
            ephemeral=True,
        )


@bot.tree.command(name="list", description="Show all your active watches")
async def list_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer(ephemeral=True)

    rows = await watch_list_for(interaction.user.id)
    if not rows:
        await interaction.followup.send(
            embed=make_embed(
                "No watches yet",
                "Use `/watch` to add one.\nExample: `/watch category:Seeds item:Bamboo minutes:5`",
            ),
            ephemeral=True,
        )
        return

    data = await CACHE.ensure_fresh()
    lines: list[str] = []
    for row in rows:
        cat  = row["category"]
        name = row["item_name"]
        nxt  = get_next(data, cat, name) if data else None
        eta  = f"in **{fmt_dur(nxt['secs'])}**" if nxt else "*no upcoming restock*"
        dest = "DM" if row["channel_id"] is None else f"<#{row['channel_id']}>"
        lines.append(
            f"{CATEGORY_EMOJI[cat]} **{name}** — alert {row['notify_minutes']}m "
            f"before · {eta} · ➜ {dest}"
        )

    embed = make_embed(
        f"📋 Your watches ({len(rows)}/{MAX_WATCHES})",
        "\n".join(lines),
        BRAND_COLOR,
    )
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="clear", description="Remove ALL your watches")
async def clear_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer(ephemeral=True)
    n = await watch_clear(interaction.user.id)
    if n == 0:
        await interaction.followup.send(
            embed=make_embed("Nothing to clear", "You had no active watches."),
            ephemeral=True,
        )
    else:
        await interaction.followup.send(
            embed=make_embed("🗑️ Cleared", f"Removed **{n}** watch(es).", SUCCESS_COLOR),
            ephemeral=True,
        )


@bot.tree.command(name="next", description="Show the next 20 upcoming restocks")
@app_commands.describe(category="Which shop to check")
@app_commands.choices(category=CATEGORY_CHOICES)
async def next_cmd(interaction: discord.Interaction,
                   category: app_commands.Choice[str]) -> None:
    await interaction.response.defer()

    data = await CACHE.ensure_fresh()
    if not data:
        await interaction.followup.send(embed=error_embed("Couldn't reach the data source."))
        return

    cat = category.value
    now_ts = int(time.time())
    results = []

    if cat == "weather":
        for ev in WEATHER_EVENTS:
            nxt = get_next_weather(data, ev, now_ts)
            if nxt:
                results.append((nxt["secs"], ev, 0, "Event", 0))
    else:
        for item in data.get(cat, []):
            nxt = get_next_restock(data, cat, item["name"], now_ts)
            if nxt:
                results.append((nxt["secs"], item["name"], nxt["qty"],
                                item.get("rarity", ""), item.get("price", 0)))

    results.sort()

    if not results:
        await interaction.followup.send(
            embed=make_embed(f"{CATEGORY_EMOJI[cat]} {cat.title()} — no upcoming restocks",
                             "Nothing predicted in the next window."),
        )
        return

    lines = []
    for secs, name, qty, rarity, price in results[:20]:
        qty_str   = f"x{qty}" if qty else "—"
        price_str = f"{fmt_price(price)}¢" if price else ""
        right     = f" · *{rarity}*" if rarity != "Event" else ""
        if price_str:
            right += f" · {price_str}"
        lines.append(f"`{fmt_dur(secs):>8}` · **{name}** · {qty_str}{right}")

    await interaction.followup.send(
        embed=make_embed(f"{CATEGORY_EMOJI[cat]} Next restocks — {cat}",
                         "\n".join(lines), INFO_COLOR),
    )


@bot.tree.command(name="info", description="Show detailed info about an item")
@app_commands.describe(category="Category", item="Item name")
@app_commands.choices(category=CATEGORY_CHOICES)
@app_commands.autocomplete(item=item_autocomplete)
async def info_cmd(interaction: discord.Interaction,
                   category: app_commands.Choice[str],
                   item: str) -> None:
    await interaction.response.defer()

    data = await CACHE.ensure_fresh()
    if not data:
        await interaction.followup.send(embed=error_embed("Couldn't reach the data source."))
        return

    cat = category.value
    valid_names = CACHE.names.get(cat, [])
    match = next((n for n in valid_names if n.lower() == item.lower()), None) \
            or next((n for n in valid_names if item.lower() in n.lower()), None)
    if not match:
        await interaction.followup.send(embed=error_embed(f"Unknown item `{item}`."))
        return

    nxt  = get_next(data, cat, match)
    meta = find_item_meta(data, cat, match)
    color = RARITY_COLORS.get(meta.get("rarity", ""), BRAND_COLOR)

    fields = []
    if cat != "weather":
        fields.append(("Rarity", meta.get("rarity", "?"), True))
        fields.append(("Price",  f"{fmt_price(meta.get('price', 0))}¢", True))

    if nxt:
        ts = nxt["restock_ts"]
        when = f"<t:{ts}:R> (<t:{ts}:T>)"
        qty_str = f"x{nxt['qty']}" if cat != "weather" else "—"
        fields.append(("Next restock", when, False))
        fields.append(("Quantity", qty_str, True))
        fields.append(("In", fmt_dur(nxt["secs"]), True))
    else:
        fields.append(("Next restock", "*Not in prediction window*", False))

    embed = make_embed(f"{CATEGORY_EMOJI[cat]} {match}", color=color)
    for name, value, inline in fields:
        embed.add_field(name=name, value=value, inline=inline)
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="items", description="List all available items in a category")
@app_commands.describe(category="Which list to show")
@app_commands.choices(category=CATEGORY_CHOICES)
async def items_cmd(interaction: discord.Interaction,
                    category: app_commands.Choice[str]) -> None:
    await interaction.response.defer(ephemeral=True)
    cat = category.value
    names = CACHE.names.get(cat, [])
    if not names:
        await interaction.followup.send(embed=error_embed("No data loaded yet — try again in a moment."),
                                        ephemeral=True)
        return
    desc = ", ".join(f"`{n}`" for n in names)
    await interaction.followup.send(
        embed=make_embed(f"{CATEGORY_EMOJI[cat]} {cat.title()} — {len(names)} items", desc),
        ephemeral=True,
    )


@bot.tree.command(name="stats", description="Show bot statistics")
async def stats_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer(ephemeral=True)

    total_watches = (await db_execute("SELECT COUNT(*) AS c FROM watches"))[0]["c"]
    unique_users  = (await db_execute("SELECT COUNT(DISTINCT user_id) AS c FROM watches"))[0]["c"]
    cache_age = int(time.time() - CACHE.fetched_at) if CACHE.fetched_at else -1

    embed = make_embed("📊 Bot stats", color=BRAND_COLOR)
    embed.add_field(name="Servers",        value=str(len(bot.guilds)), inline=True)
    embed.add_field(name="Active watches", value=str(total_watches),   inline=True)
    embed.add_field(name="Unique users",   value=str(unique_users),    inline=True)
    embed.add_field(name="Cache age",
                    value=f"{cache_age}s ago" if cache_age >= 0 else "never",
                    inline=True)
    embed.add_field(name="Latency", value=f"{round(bot.latency * 1000)} ms", inline=True)
    embed.add_field(name="Data items",
                    value=f"🌱 {len(CACHE.names['seeds'])} · ⚙️ {len(CACHE.names['gears'])}",
                    inline=True)
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="help", description="How to use this bot")
async def help_cmd(interaction: discord.Interaction) -> None:
    embed = make_embed(
        "🌱 Grow a Garden — Stock Predictor",
        "Get notified before any seed, gear, or weather event restocks.",
        BRAND_COLOR,
    )
    embed.add_field(
        name="📌 Quick start",
        value=(
            "`/watch category:Seeds item:Bamboo minutes:5`\n"
            "→ DM you 5 minutes before Bamboo restocks.\n\n"
            "Run it inside a channel and you'll be pinged there instead."
        ),
        inline=False,
    )
    embed.add_field(
        name="🛠️ Commands",
        value=(
            "`/watch` · add or update a watch\n"
            "`/unwatch` · remove a watch\n"
            "`/list` · your active watches\n"
            "`/clear` · remove all your watches\n"
            "`/next` · 20 nearest upcoming restocks\n"
            "`/info` · details on a single item\n"
            "`/items` · full item list per category\n"
            "`/stats` · bot statistics"
        ),
        inline=False,
    )
    embed.add_field(
        name="ℹ️ Notes",
        value=(
            f"• Max **{MAX_WATCHES}** watches per user.\n"
            f"• Notify time: **{MIN_NOTIFY_MIN}–{MAX_NOTIFY_MIN}** minutes.\n"
            "• Predictions are sourced from luminon.top/gag2.\n"
            "• Server-driven events (Goldmoon outside schedule, etc.) cannot be forecast."
        ),
        inline=False,
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


# ─── ERROR HANDLER ────────────────────────────────────────────────────────────

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction,
                               error: app_commands.AppCommandError) -> None:
    if isinstance(error, app_commands.CommandOnCooldown):
        msg = f"Slow down — try again in {error.retry_after:.0f}s."
    elif isinstance(error, app_commands.MissingPermissions):
        msg = "You don't have permission to do that."
    else:
        log.exception("Unhandled app command error", exc_info=error)
        msg = "Something went wrong. Please try again."

    try:
        if interaction.response.is_done():
            await interaction.followup.send(embed=error_embed(msg), ephemeral=True)
        else:
            await interaction.response.send_message(embed=error_embed(msg), ephemeral=True)
    except discord.HTTPException:
        pass


# ─── BACKGROUND NOTIFICATION LOOP ─────────────────────────────────────────────

@tasks.loop(seconds=NOTIFY_TICK)
async def notification_loop() -> None:
    """
    Periodically:
      • refresh the shared cache if it's stale,
      • iterate every active watch,
      • for any watch whose next restock is within `notify_minutes`, send the alert,
      • mark it so we don't ping again for that same restock.
    """
    try:
        # Make sure data is fresh enough; ensure_fresh() decides whether to refetch.
        data = await CACHE.ensure_fresh()
        if not data:
            return

        now_ts = int(time.time())
        watches = await watch_all()
        if not watches:
            return

        # Batch fetched channels to avoid repeated bot.get_channel calls
        channel_cache: dict[int, Any] = {}

        for w in watches:
            uid       = w["user_id"]
            cat       = w["category"]
            name      = w["item_name"]
            threshold = w["notify_minutes"] * 60

            nxt = get_next(data, cat, name, now_ts)
            if not nxt or not (0 < nxt["secs"] <= threshold):
                continue
            if await notif_seen(uid, cat, name, nxt["restock_ts"]):
                continue

            # Build the notification embed
            meta = find_item_meta(data, cat, name)
            color = RARITY_COLORS.get(meta.get("rarity", ""), BRAND_COLOR)
            ts = nxt["restock_ts"]

            embed = make_embed(
                f"🔔 {name} restocks in {fmt_dur(nxt['secs'])}",
                color=color,
            )
            embed.add_field(name="Category", value=f"{CATEGORY_EMOJI[cat]} {cat.title()}", inline=True)
            if cat != "weather":
                embed.add_field(name="Quantity", value=f"x{nxt['qty']}", inline=True)
                embed.add_field(name="Price",    value=f"{fmt_price(meta.get('price', 0))}¢", inline=True)
                embed.add_field(name="Rarity",   value=meta.get("rarity", "?"), inline=True)
            embed.add_field(name="At",  value=f"<t:{ts}:T> (<t:{ts}:R>)", inline=False)

            # Resolve destination — channel mention if set, otherwise DM
            sent = False
            try:
                if w["channel_id"]:
                    ch = channel_cache.get(w["channel_id"])
                    if ch is None:
                        ch = bot.get_channel(w["channel_id"]) or \
                             await bot.fetch_channel(w["channel_id"])
                        channel_cache[w["channel_id"]] = ch
                    await ch.send(content=f"<@{uid}>", embed=embed,
                                  allowed_mentions=discord.AllowedMentions(users=True))
                else:
                    user = bot.get_user(uid) or await bot.fetch_user(uid)
                    await user.send(embed=embed)
                sent = True
            except discord.Forbidden:
                log.warning("Forbidden when notifying user=%s; removing destination", uid)
                # Channel deleted / DMs closed — clean up this watch
                await db_execute("DELETE FROM watches WHERE id=?", (w["id"],))
            except discord.NotFound:
                log.warning("Channel/user not found for watch id=%s; removing", w["id"])
                await db_execute("DELETE FROM watches WHERE id=?", (w["id"],))
            except Exception:
                log.exception("Notification send failed for watch id=%s", w["id"])

            if sent:
                await notif_mark(uid, cat, name, nxt["restock_ts"])

        # Periodic cleanup (cheap, but only every ~5 min on average)
        if int(time.time()) % 300 < NOTIFY_TICK:
            await notif_cleanup()

    except Exception:
        log.exception("Notification loop crashed")


@notification_loop.before_loop
async def _wait_for_ready() -> None:
    await bot.wait_until_ready()


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main() -> None:
    if not TOKEN:
        print("ERROR: set the DISCORD_TOKEN environment variable before starting.",
              file=sys.stderr)
        sys.exit(1)

    db_init()

    try:
        bot.run(TOKEN, log_handler=None)   # we have our own logging
    finally:
        if _db_conn is not None:
            _db_conn.close()


if __name__ == "__main__":
    main()
