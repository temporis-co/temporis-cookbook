from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pathlib import Path

router = APIRouter()

async def parse_response(response):
    if not response.ok:
        return JSONResponse(content={'status': response.status})
    return await response.json(content_type=None)

HERE = Path(__file__).parent
INDEX_HTML = (HERE / 'index.html').read_text()
SYMBOL_SEARCH_HTML = (HERE / 'symbol_search.html').read_text()
SECURITY_DEFINITION_HTML = (HERE / 'security_definition.html').read_text()
CONTRACT_INFO_RULES_HTML = (HERE / 'contract_info_rules.html').read_text()
TRADING_SCHEDULE_HTML = (HERE / 'trading_schedule.html').read_text()
WS_MONITOR_HTML = (HERE / 'ws_monitor.html').read_text()
LOGOUT_HTML = (HERE / 'logout.html').read_text()
TICKLE_HTML = (HERE / 'tickle.html').read_text()


# Home page
@router.get('/', response_class=HTMLResponse)
async def index():
    return INDEX_HTML


# 0. Search Contract by Symbol
@router.get('/symbol-search', response_class=HTMLResponse)
async def symbol_search():
    return SYMBOL_SEARCH_HTML


@router.get('/api/symbol-search')
async def api_symbol_search(request: Request, symbol: str):
    client = request.app.state.client
    api_url = request.app.state.api_url
    async with client.get(api_url + '/iserver/secdef/search', params={'symbol': symbol}, ssl=False) as response:
        return await parse_response(response)


# 1. Security Definition by Contract ID
@router.get('/security-definition', response_class=HTMLResponse)
async def security_definition():
    return SECURITY_DEFINITION_HTML


@router.get('/api/security-definition')
async def api_security_definition(request: Request, conids: str):
    client = request.app.state.client
    api_url = request.app.state.api_url
    async with client.get(api_url + '/trsrv/secdef', params={'conids': conids}, ssl=False) as response:
        return await parse_response(response)


# 2. Info and Rules for Contract
@router.get('/contract-info-rules', response_class=HTMLResponse)
async def contract_info_rules():
    return CONTRACT_INFO_RULES_HTML


@router.get('/api/contract-info-rules')
async def api_contract_info_rules(request: Request, conid: str):
    client = request.app.state.client
    api_url = request.app.state.api_url
    async with client.get(api_url + f'/iserver/contract/{conid}/info-and-rules', ssl=False) as response:
        return await parse_response(response)


# 3. Trading Schedule
@router.get('/trading-schedule', response_class=HTMLResponse)
async def trading_schedule():
    return TRADING_SCHEDULE_HTML


@router.get('/api/trading-schedule')
async def api_trading_schedule(request: Request, conid: str):
    client = request.app.state.client
    api_url = request.app.state.api_url
    async with client.get(api_url + '/contract/trading-schedule', params={'conid': conid}, ssl=False) as response:
        return await parse_response(response)


# 4. WebSocket Monitor
@router.get('/ws-monitor', response_class=HTMLResponse)
async def ws_monitor():
    return WS_MONITOR_HTML


# 5. Logout
@router.get('/logout', response_class=HTMLResponse)
async def logout_page():
    return LOGOUT_HTML


@router.post('/api/logout')
async def api_logout(request: Request):
    client = request.app.state.client
    api_url = request.app.state.api_url
    async with client.post(api_url + '/logout', json={}, ssl=False) as response:
        return await parse_response(response)


# 6. Tickle
@router.get('/tickle', response_class=HTMLResponse)
async def tickle_page():
    return TICKLE_HTML


@router.post('/api/tickle')
async def api_tickle(request: Request):
    client = request.app.state.client
    api_url = request.app.state.api_url
    async with client.post(api_url + '/tickle', json={}, ssl=False) as response:
        return await parse_response(response)
