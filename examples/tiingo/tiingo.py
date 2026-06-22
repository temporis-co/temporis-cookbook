import asyncio
import json
import logging
import os
import signal
import ssl
import time
from datetime import datetime
from urllib import error, request

import certifi
from dotenv import load_dotenv
from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

TIINGO_IEX_WS_URL = "wss://api.tiingo.com/iex"
TEMPORIS_INGEST_URL = "https://api.temporis.co/v1/data_sources/ingest"
THRESHOLD_LEVEL = 6
MESSAGE_TIMEOUT_SECONDS = 300
RECONNECT_DELAY_SECONDS = 5
FLUSH_INTERVAL_SECONDS = 10
INGEST_TIMEOUT_SECONDS = 10
SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def load_config():
    load_dotenv()
    with open("config.json") as f:
        config = json.load(f)
    config["tiingo_token"] = os.environ["TIINGO_TOKEN"]
    config["temporis"]["access_token"] = os.environ["TS_TOKEN"]
    return config


def parse_event(data):
    try:
        event_time = int(datetime.fromisoformat(data[0]).timestamp())
        ticker = data[1].strip().upper()
        price = float(data[2])
    except Exception:
        return None
    if not ticker or price <= 0:
        return None
    return event_time, ticker, price


def ingest_rows(config, rows):
    if not rows:
        return

    payload = {
        "data_source": config["temporis"]["data_source"],
        "records": [
            {"timestamp": event_time, "name": name, "value": value}
            for event_time, name, value in rows
        ],
    }
    body = json.dumps(payload).encode("utf-8")
    ingest_request = request.Request(
        TEMPORIS_INGEST_URL,
        data=body,
        headers={
            "Authorization": f'Bearer {config["temporis"]["access_token"]}',
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with request.urlopen(ingest_request, timeout=INGEST_TIMEOUT_SECONDS, context=SSL_CONTEXT) as response:
            if response.status != 204:
                response_body = response.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"ingest failed with {response.status}: {response_body}")
    except error.HTTPError as exc:
        response_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ingest failed with {exc.code}: {response_body}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"ingest request failed: {exc.reason}") from exc

    rows.clear()


async def run():
    config = load_config()
    tickers = [ticker.strip().upper() for ticker in config["tickers"] if ticker.strip()]

    rows = []
    last_flush = time.monotonic()
    while True:
        try:
            async with connect(
                TIINGO_IEX_WS_URL,
                ssl=SSL_CONTEXT,
                ping_interval=30,
                ping_timeout=10,
                open_timeout=20,
                close_timeout=5,
            ) as ws:
                await ws.send(json.dumps({
                    "eventName": "subscribe",
                    "authorization": config["tiingo_token"],
                    "eventData": {"tickers": tickers, "thresholdLevel": THRESHOLD_LEVEL},
                }))
                logger.info("subscribed to %s tickers", len(tickers))
                try:
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=MESSAGE_TIMEOUT_SECONDS)
                        message = json.loads(raw)
                        if message.get("messageType") == "A":
                            event = parse_event(message["data"])
                            if event:
                                rows.append(event)
                        elif message.get("messageType") == "I":
                            logger.info("%s", message["response"]["message"])
                        elif message.get("messageType") == "E":
                            logger.warning("%s", message)

                        now = time.monotonic()
                        if rows and now - last_flush >= FLUSH_INTERVAL_SECONDS:
                            try:
                                ingest_rows(config, rows)
                            except RuntimeError as exc:
                                logger.warning("%s", exc)
                                rows.clear()
                            last_flush = time.monotonic()
                except asyncio.TimeoutError:
                    logger.warning("message timeout, reconnecting")
                    raise
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)
        except (OSError, WebSocketException) as exc:
            logger.warning("reconnecting after error: %s", exc)
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)


async def main():
    task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, task.cancel)
    try:
        await run()
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
