from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib import error, request

from dotenv import load_dotenv
import quickfix as fix
import quickfix44 as fix44


DICTIONARY_PATH = Path(__file__).with_name("DUKAFIX44.xml").resolve()
INGEST_ENDPOINT = "https://api.temporis.co/v1/data_sources/ingest"
INGEST_TIMEOUT_SECONDS = 5

DUKASCOPY_HOST = "demo-api.dukascopy.com"
DUKASCOPY_PORT = 9443
DUKASCOPY_TARGET_COMP_ID = "DUKASCOPYFIX"
DUKASCOPY_HEART_BT_INT = 30
DUKASCOPY_RECONNECT_INTERVAL = 60
LOGGER = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )


def log_info(message: str) -> None:
    LOGGER.info(message)


def log_error(message: str) -> None:
    LOGGER.error(message)


def log_warning(message: str) -> None:
    LOGGER.warning(message)


def load_config() -> dict:
    load_dotenv(".env")
    with open("config.json") as f:
        config = json.load(f)

    config["dukascopy"]["password"] = os.environ["DUKASCOPY_PASSWORD"]
    config["temporis"]["access_token"] = os.environ["TS_TOKEN"]
    config["pairs"] = tuple(pair.strip().upper() for pair in config["pairs"])
    return config


def entry_name(symbol: str, entry_type: str) -> str | None:
    side = {"0": "bid_price", "1": "ask_price"}.get(entry_type)
    if side is None:
        return None
    return f"{symbol.replace('/', '_')}__{side}"


def parse_vendor_timestamp(sending_time_value: str, entry_time_value: str) -> int:
    date_part = sending_time_value.split("-", 1)[0]
    timestamp_text = f"{date_part}-{entry_time_value}"
    for fmt in ("%Y%m%d-%H:%M:%S.%f", "%Y%m%d-%H:%M:%S"):
        try:
            dt = datetime.strptime(timestamp_text, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            pass
    raise ValueError(f"Bad vendor timestamp: {timestamp_text}")


def ingest_records(data_source: str, access_token: str, rows: list[tuple[int, str, float]]) -> None:
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


class DukascopyApp(fix.Application):
    def __init__(self, username: str, password: str, data_source: str, access_token: str, pairs: tuple[str, ...]):
        super().__init__()
        self.username = username
        self.password = password
        self.data_source = data_source
        self.access_token = access_token
        self.pairs = pairs

    def onCreate(self, session_id):
        pass

    def onLogon(self, session_id):
        log_info(f"onLogon: {session_id.getSenderCompID().getString()}")
        self.subscribe(session_id)

    def onLogout(self, session_id):
        pass

    def toAdmin(self, message, session_id):
        msg_type = fix.MsgType()
        message.getHeader().getField(msg_type)
        if msg_type.getValue() == "A":
            message.setField(fix.EncryptMethod(0))
            message.setField(fix.HeartBtInt(DUKASCOPY_HEART_BT_INT))
            message.setField(fix.ResetSeqNumFlag(True))
            message.setField(fix.Username(self.username))
            message.setField(fix.Password(self.password))

    def toApp(self, message, session_id):
        pass

    def fromAdmin(self, message, session_id):
        msg_type = fix.MsgType()
        message.getHeader().getField(msg_type)
        if msg_type.getValue() == "5":
            if message.isSetField(fix.Text().getField()):
                text = fix.Text()
                message.getField(text)
                reason = text.getValue()
                if reason:
                    log_warning(f"Dukascopy logout: {reason}")

    def fromApp(self, message, session_id):
        msg_type = fix.MsgType()
        message.getHeader().getField(msg_type)
        if msg_type.getValue() == "W":
            self.process_market_data(message)
        elif msg_type.getValue() == "Y":
            self.process_market_data_reject(message)

    def subscribe(self, session_id):
        for pair in self.pairs:
            request = fix44.MarketDataRequest()
            request.setField(fix.MDReqID(f"MDREQ_{pair.replace('/', '_')}"))
            request.setField(fix.SubscriptionRequestType("1"))
            request.setField(fix.MarketDepth(1))
            request.setField(fix.MDUpdateType(0))

            for entry_type in ("0", "1"):
                entry_group = fix44.MarketDataRequest.NoMDEntryTypes()
                entry_group.setField(fix.MDEntryType(entry_type))
                request.addGroup(entry_group)

            symbol_group = fix44.MarketDataRequest.NoRelatedSym()
            symbol_group.setField(fix.Symbol(pair))
            request.addGroup(symbol_group)

            fix.Session.sendToTarget(request, session_id)
        log_info(f"Subscribed to {len(self.pairs)} pairs.")

    def process_market_data(self, message):
        try:
            symbol = fix.Symbol()
            message.getField(symbol)
            sending_time = fix.SendingTime()
            message.getHeader().getField(sending_time)
            no_entries = fix.NoMDEntries()
            message.getField(no_entries)
        except fix.FieldNotFound:
            return

        rows = []

        for index in range(1, int(no_entries.getValue()) + 1):
            group = fix44.MarketDataSnapshotFullRefresh.NoMDEntries()
            message.getGroup(index, group)

            try:
                entry_type = fix.MDEntryType()
                entry_px = fix.MDEntryPx()
                entry_time = fix.MDEntryTime()
                group.getField(entry_type)
                group.getField(entry_px)
                group.getField(entry_time)
                name = entry_name(symbol.getValue(), entry_type.getValue())
                if name is None:
                    continue
                updated = parse_vendor_timestamp(
                    sending_time.getString(), entry_time.getString()
                )
                value = float(entry_px.getValue())
            except (fix.FieldNotFound, ValueError):
                continue

            rows.append((updated, name, value))

        if not rows:
            return

        try:
            ingest_records(self.data_source, self.access_token, rows)
        except Exception as exc:
            log_warning(f"Temporis ingest failed for {symbol.getValue()} with {len(rows)} rows: {exc}")

    def process_market_data_reject(self, message):
        md_req_id = fix.MDReqID()
        reason = fix.MDReqRejReason()
        if message.isSetField(md_req_id):
            message.getField(md_req_id)
        if message.isSetField(reason):
            message.getField(reason)
        error_message = f"Market data request rejected: md_req_id={md_req_id.getValue()} reason={reason.getValue()}"
        log_error(error_message)


def build_session_settings(config: dict) -> fix.SessionSettings:
    settings = fix.SessionSettings()

    defaults = fix.Dictionary()
    defaults.setString("ConnectionType", "initiator")
    defaults.setString("StartTime", "00:00:00")
    defaults.setString("EndTime", "00:00:00")
    defaults.setString("HeartBtInt", str(DUKASCOPY_HEART_BT_INT))
    defaults.setString("ReconnectInterval", str(DUKASCOPY_RECONNECT_INTERVAL))
    defaults.setString("DataDictionary", str(DICTIONARY_PATH))
    defaults.setString("ScreenLogShowIncoming", "N")
    defaults.setString("ScreenLogShowOutgoing", "N")
    defaults.setString("ScreenLogShowEvents", "N")
    settings.set(defaults)

    session_id = fix.SessionID("FIX.4.4", config["dukascopy"]["sender_comp_id"], DUKASCOPY_TARGET_COMP_ID)
    session = fix.Dictionary()
    session.setString("SocketConnectHost", DUKASCOPY_HOST)
    session.setString("SocketConnectPort", str(DUKASCOPY_PORT))
    settings.set(session_id, session)

    return settings

def main() -> int:
    configure_logging()

    try:
        config = load_config()
    except Exception as exc:
        log_error(f"Configuration error: {exc}")
        return 1

    try:
        settings = build_session_settings(config)
        app = DukascopyApp(
            username=config["dukascopy"]["username"],
            password=config["dukascopy"]["password"],
            data_source=config["temporis"]["data_source"],
            access_token=config["temporis"]["access_token"],
            pairs=config["pairs"],
        )

        store_factory = fix.MemoryStoreFactory()
        log_factory = fix.ScreenLogFactory(settings)
        initiator = fix.SSLSocketInitiator(app, store_factory, settings, log_factory)

        log_info(f"Starting Dukascopy FIX initiator for {', '.join(config['pairs'])}...")
        initiator.start()

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        log_error(f"Startup failed: {exc}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
