from fastapi import APIRouter, WebSocket
from urllib import error, request
import asyncio
import json
import time

INGEST_ENDPOINT = "https://api.temporis.co/v1/data_sources/ingest"
INGEST_TIMEOUT_SECONDS = 5

connections = set()
router = APIRouter()

@router.websocket('/')
async def live(ws: WebSocket):
    await ws.accept()
    connections.add(ws)
    try:
        while True:
            await ws.receive()
    except Exception:
        pass

async def broadcast(data):
    disconnected = []
    message = json.dumps(data)
    for ws in connections:
        try:
            await ws.send_text(message)
        except Exception:
            disconnected.append(ws)
    connections.difference_update(disconnected)

async def receive_market_data(client, ws_url, state):
    try:
        ws = await client.ws_connect(ws_url, ssl=False)
        symbol, price, temporis = state['symbol'], state['price'], state['temporis']
        last_time, timeout = time.monotonic(), 300
        while True:
            message = await asyncio.wait_for(ws.receive(), timeout)
            data = json.loads(message.data)
            print(data)

            current_time = time.monotonic()
            if current_time - last_time > timeout:
                break

            if data['topic'] == 'sts' and data['args'].get('username'):
                for conid in symbol.keys():
                    await ws.send_str(f'smd+{conid}+{{"fields":["84","86"]}}')

            if '_updated' not in data:
                continue
            updated = data['_updated'] // 1000

            timestamp = state['timestamp']
            if updated > timestamp:
                rows = []
                for name, item in price.items():
                    if item['updated'] < timestamp:
                        continue
                    await broadcast({
                        'event_time': timestamp,
                        'name': name,
                        'value': item['value'],
                    })
                    rows.append((timestamp, name, item['value']))
                ingest_records(temporis['data_source'], temporis['access_token'], rows)
                state['timestamp'] = updated

            if 'conid' not in data:
                continue
            conid = data['conid']
            if '86' in data and data['86']:
                price[f'{symbol[conid]}__ask_price'] = {'value': float(data['86']), 'updated': updated}
            if '84' in data and data['84']:
                price[f'{symbol[conid]}__bid_price'] = {'value': float(data['84']), 'updated': updated}

            last_time = current_time
    except Exception as e:
        print(e)
    finally:
        await ws.close()
        print('exit')

def ingest_records(data_source, access_token, rows):
    if not rows:
        return
    payload = {
        "data_source": data_source,
        "records": [
            {"timestamp": timestamp, "name": name, "value": value}
            for timestamp, name, value in rows
        ],
    }
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        INGEST_ENDPOINT,
        data=body,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        request.urlopen(req, timeout=INGEST_TIMEOUT_SECONDS).close()
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Ingest request failed with status {exc.code}: {details}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Unable to reach ingest endpoint: {exc.reason}") from exc
