# scanner.py
"""Polymarket slug-based scanner for active 15-minute crypto markets."""

import asyncio
import json
from datetime import datetime, timezone

import aiohttp
from colorama import Fore, Style, init

from config import Config

# Initialize colorama
init(autoreset=True)

GAMMA_MARKET_URL = "https://gamma-api.polymarket.com/markets"

ASSETS = ["btc", "eth", "sol"]   # XRP removed: no Chainlink oracle feed
INTERVAL_SECONDS = 900          # 15 minutes
REFRESH_INTERVAL_SECONDS = 30
RETRY_WAIT_SECONDS = 10
MAX_TIME_REMAINING_MINUTES = 30  # accept current + next upcoming market

ACTIVE_MARKETS: dict[str, dict] = {}


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def get_active_markets() -> dict[str, dict]:
    """Return the current ACTIVE_MARKETS dict."""
    return ACTIVE_MARKETS


def get_market_count() -> int:
    """Return the number of currently tracked markets."""
    return len(ACTIVE_MARKETS)


def prune_expired_markets() -> None:
    """Remove markets that have been expired for more than 15 seconds."""
    to_remove = [
        cid for cid, mkt in ACTIVE_MARKETS.items()
        if _time_remaining_minutes(mkt.get("end_date", "")) * 60 < -15
    ]
    for cid in to_remove:
        del ACTIVE_MARKETS[cid]
    if to_remove:
        print(f"{Fore.WHITE}[SCANNER] Pruned {len(to_remove)} expired market(s) from tracking.")


# ---------------------------------------------------------------------------
# Slug + timestamp helpers
# ---------------------------------------------------------------------------

def _current_slot_ts() -> int:
    """Return Unix timestamp of the current 15-min slot (floor to boundary)."""
    now_utc = datetime.now(timezone.utc)
    minutes_rounded = (now_utc.minute // 15) * 15
    slot_time = now_utc.replace(minute=minutes_rounded, second=0, microsecond=0)
    return int(slot_time.timestamp())


def _slugs_to_fetch() -> list[tuple[str, str]]:
    """Return (asset, slug) pairs for 4 time offsets × 3 assets = 12 slugs."""
    base = _current_slot_ts()
    offsets = (0, INTERVAL_SECONDS, -INTERVAL_SECONDS, 2 * INTERVAL_SECONDS)
    pairs: list[tuple[str, str]] = []
    for asset in ASSETS:
        for offset in offsets:
            ts = base + offset
            pairs.append((asset, f"{asset}-updown-15m-{ts}"))
    return pairs


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _time_remaining_minutes(end_date_str: str) -> float:
    """Return minutes until end_date_str from now (UTC). Negative = expired."""
    try:
        end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        return (end_dt - datetime.now(timezone.utc)).total_seconds() / 60.0
    except (ValueError, AttributeError, TypeError):
        return 0.0


def _parse_json_field(value) -> list:
    """Parse a field that may already be a list or a JSON-encoded string."""
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            result = json.loads(value)
            return result if isinstance(result, list) else []
        except (json.JSONDecodeError, ValueError):
            return []
    return []


def _parse_market(raw: dict, symbol: str, slug: str) -> dict | None:
    """Parse a Gamma API market dict into our internal schema."""
    if not raw.get("active", False) or raw.get("closed", False):
        return None

    condition_id: str = raw.get("conditionId", raw.get("condition_id", ""))
    if not condition_id:
        return None

    end_date: str = raw.get("endDate", raw.get("end_date_iso", raw.get("endDateIso", "")))
    time_remaining = _time_remaining_minutes(end_date)
    time_remaining_seconds = time_remaining * 60

    if time_remaining > MAX_TIME_REMAINING_MINUTES:
        return None
    if time_remaining_seconds < -15:
        return None

    clob_ids = _parse_json_field(raw.get("clobTokenIds", []))
    if len(clob_ids) < 2:
        return None
    up_token_id = str(clob_ids[0])
    down_token_id = str(clob_ids[1])

    outcome_prices = _parse_json_field(raw.get("outcomePrices", []))
    try:
        up_price = float(outcome_prices[0]) if len(outcome_prices) > 0 else 0.0
        down_price = float(outcome_prices[1]) if len(outcome_prices) > 1 else 0.0
    except (ValueError, TypeError):
        up_price = down_price = 0.0

    try:
        volume = float(raw.get("volume", 0.0))
    except (ValueError, TypeError):
        volume = 0.0

    return {
        "condition_id": condition_id,
        "question": raw.get("question", ""),
        "symbol": symbol,
        "up_token_id": up_token_id,
        "down_token_id": down_token_id,
        "up_price": up_price,
        "down_price": down_price,
        "time_remaining_minutes": round(time_remaining, 2),
        "time_remaining_seconds": round(time_remaining_seconds, 1),
        "end_date": end_date,
        "volume": volume,
        "slug": slug,
        "price_to_beat": 0.0,
    }


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

async def _fetch_slug(session: aiohttp.ClientSession, slug: str) -> dict | None:
    """GET /markets?slug={slug} and return first result or None."""
    async with session.get(GAMMA_MARKET_URL, params={"slug": slug}) as resp:
        resp.raise_for_status()
        data = await resp.json()
    if isinstance(data, list) and data:
        return data[0]
    return None


# ---------------------------------------------------------------------------
# Refresh loop
# ---------------------------------------------------------------------------

async def _refresh_once(session: aiohttp.ClientSession) -> None:
    """Fetch all 12 slugs, parse, rebuild ACTIVE_MARKETS."""
    global ACTIVE_MARKETS

    slugs = _slugs_to_fetch()
    updated: dict[str, dict] = {}

    for asset, slug in slugs:
        try:
            raw = await _fetch_slug(session, slug)
        except aiohttp.ClientError:
            continue

        if raw is None:
            continue

        parsed = _parse_market(raw, asset, slug)

        if parsed:
            updated[parsed["condition_id"]] = parsed
            print(
                f"{Fore.GREEN}[SCANNER] {slug} "
                f"→ {parsed['time_remaining_minutes']:.1f} min "
                f"({parsed['time_remaining_seconds']:.0f}s) remaining  ACCEPTED"
            )

    ACTIVE_MARKETS = updated

    if ACTIVE_MARKETS:
        print(
            f"{Fore.GREEN}{Style.BRIGHT}[SCANNER] "
            f"{len(ACTIVE_MARKETS)} active market(s) tracked."
        )
    else:
        print(f"{Fore.YELLOW}[SCANNER] No active 15-min markets found this cycle.")


async def run_scanner() -> None:
    """Continuously refresh ACTIVE_MARKETS every 60 seconds."""
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await _refresh_once(session)
                prune_expired_markets()
            except Exception as exc:  # pylint: disable=broad-except
                print(f"{Fore.RED}[SCANNER] Unexpected error: {exc} — retrying in {RETRY_WAIT_SECONDS}s...")
                await asyncio.sleep(RETRY_WAIT_SECONDS)
                continue

            await asyncio.sleep(REFRESH_INTERVAL_SECONDS)


if __name__ == "__main__":
    Config.summary()
    asyncio.run(run_scanner())