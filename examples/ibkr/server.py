from fastapi import FastAPI
from dotenv import load_dotenv
from http_router import router as http_router
from ws_router import receive_market_data, router as ws_router
import aiohttp
import asyncio
import json
import os

app = FastAPI()
app.include_router(http_router)
app.include_router(ws_router)

@app.on_event('startup')
async def startup():
    load_dotenv()
    with open('config.json', 'r') as f:
        config = json.load(f)
        config['temporis']['access_token'] = os.environ['TS_TOKEN']
        host = config['client_portal']['host']
        port = config['client_portal']['port']
        api_url = f'https://{host}:{port}/v1/api'
        ws_url = f'wss://{host}:{port}/v1/api/ws'
        state = {'timestamp': 0, 'price': {}, 'temporis': config['temporis'], 'symbol': {c['conid']: c['symbol'] for c in config['contracts']}}

    app.state.api_url = api_url
    app.state.client = aiohttp.ClientSession()
    asyncio.create_task(maintain_session(app.state.client, api_url, ws_url, state))

async def maintain_session(client, api_url, ws_url, state):
    task = type('Task', (), {'done': lambda self: True})()
    while True:
        try:
            await asyncio.sleep(60)
            async with client.post(api_url + '/tickle', json={}, ssl=False) as response:
                if not response.ok:
                    continue
                print('tickle')
                data = await response.json()
                authenticated = data['iserver']['authStatus']['authenticated']
                if authenticated and task.done():
                    task = asyncio.create_task(receive_market_data(client, ws_url, state))
        except Exception:
            pass
