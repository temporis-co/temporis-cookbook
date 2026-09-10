import asyncio
import json
import logging
import os
import ssl
import time
from datetime import datetime, timezone

import certifi
import requests
import websockets
from dotenv import load_dotenv
from websockets.exceptions import WebSocketException

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
TEMPORIS_INGEST_URL = "https://api.temporis.co/v1/data_sources/ingest"

DISCOVERY_SECONDS = 60
PING_SECONDS = 10
FLUSH_SECONDS = 10
INGEST_TIMEOUT_SECONDS = 10


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        config = json.load(f)

    load_dotenv()
    config["temporis"]["access_token"] = os.environ["TS_TOKEN"]
    return config


def parse_time(value: str) -> int:
    return int(datetime.fromisoformat(value).timestamp())


def market_name(series_slug: str, outcome: str) -> str:
    suffix = "-up-or-down-hourly"
    if series_slug.endswith(suffix):
        asset = series_slug.removesuffix(suffix)
        return f"{asset}_hourly_{outcome.lower()}"
    return f"{series_slug.replace('-', '_')}_{outcome.lower()}"


def discover(series_slugs: list[str]) -> dict[str, tuple[str, int, int]]:
    now = int(datetime.now(tz=timezone.utc).timestamp())
    tokens = {}
    for series_slug in series_slugs:
        try:
            response = requests.get(
                GAMMA_EVENTS_URL,
                params={"series_slug": series_slug, "closed": "false", "limit": 20},
                timeout=20,
            )
            response.raise_for_status()
            events = response.json()

            candidates = []
            for event in events:
                for market in event.get("markets") or []:
                    outcomes = json.loads(market["outcomes"])
                    token_ids = json.loads(market["clobTokenIds"])
                    if (
                        market.get("enableOrderBook")
                        and not market.get("closed")
                        and {str(outcome) for outcome in outcomes} == {"Up", "Down"}
                        and len(token_ids) == 2
                    ):
                        candidates.append((parse_time(market["eventStartTime"]), parse_time(market["endDate"]), outcomes, token_ids))

            current = max((item for item in candidates if item[0] <= now < item[1]), key=lambda item: item[0], default=None)
            upcoming = min((item for item in candidates if item[0] > now), key=lambda item: item[0], default=None)
            for start, end, outcomes, token_ids in [item for item in (current, upcoming) if item]:
                for outcome, token_id in zip(outcomes, token_ids):
                    tokens[str(token_id)] = (market_name(series_slug, str(outcome)), start, end)
        except Exception as exc:
            logging.warning("discovery failed for %s: %s", series_slug, exc)
    return tokens


def records_from_message(raw: str, tokens: dict[str, tuple[str, int, int]]) -> list[dict]:
    try:
        message = json.loads(raw)
    except json.JSONDecodeError:
        return []

    records = []
    for item in message if isinstance(message, list) else [message]:
        if not isinstance(item, dict) or item.get("event_type") != "best_bid_ask":
            continue
        token = tokens.get(str(item.get("asset_id") or ""))
        if not token:
            continue
        name, start, end = token
        ts = int(item["timestamp"]) // 1000
        if not start <= ts < end:
            continue
        records.append({"timestamp": ts, "name": f"{name}_bid", "value": float(item["best_bid"])})
        records.append({"timestamp": ts, "name": f"{name}_ask", "value": float(item["best_ask"])})
    return records


async def subscribe(ws, token_ids: list[str]) -> None:
    if not token_ids:
        return
    await ws.send(
        json.dumps({
            "assets_ids": token_ids,
            "type": "market",
            "operation": "subscribe",
            "custom_feature_enabled": True,
        })
    )


async def flush(
    data_source: str,
    access_token: str,
    records: list[dict],
) -> None:
    if not records:
        return

    def write() -> None:
        response = requests.post(
            TEMPORIS_INGEST_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            json={"data_source": data_source, "records": records},
            timeout=INGEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()

    try:
        await asyncio.to_thread(write)
    except requests.RequestException as exc:
        logging.warning("ingest failed: %s; buffered_records=%d", exc, len(records))
    records.clear()


async def collect(config: dict) -> None:
    targets = config["targets"]
    temporis = config["temporis"]
    data_source = temporis["data_source"]
    access_token = temporis["access_token"]
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    while True:
        tokens = await asyncio.to_thread(discover, targets)
        records: list[dict] = []
        discover_task = None
        flush_task = None
        try:
            async with websockets.connect(MARKET_WS_URL, ssl=ssl_context, ping_interval=None) as ws:
                await subscribe(ws, list(tokens))
                last_discovery = last_ping = last_flush = time.monotonic()

                while True:
                    now = time.monotonic()
                    if discover_task and discover_task.done():
                        found = discover_task.result()
                        if found:
                            tokens = found
                            await subscribe(ws, list(tokens))
                        discover_task = None
                    if flush_task and flush_task.done():
                        flush_task = None
                    if now - last_discovery >= DISCOVERY_SECONDS and discover_task is None:
                        discover_task = asyncio.create_task(asyncio.to_thread(discover, targets))
                        last_discovery = now
                    if now - last_ping >= PING_SECONDS:
                        await ws.send("PING")
                        last_ping = now
                    if now - last_flush >= FLUSH_SECONDS and flush_task is None and records:
                        flush_task = asyncio.create_task(flush(data_source, access_token, records))
                        records = []
                        last_flush = now

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1)
                    except asyncio.TimeoutError:
                        continue
                    records.extend(records_from_message(raw, tokens))
        except (OSError, WebSocketException) as exc:
            logging.warning(
                "market websocket session ended (%s): %s; buffered_records=%d tracked_tokens=%d",
                type(exc).__name__,
                exc,
                len(records),
                len(tokens),
            )
            await asyncio.sleep(5)


async def amain() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    await collect(load_config("config.json"))


if __name__ == "__main__":
    asyncio.run(amain())
