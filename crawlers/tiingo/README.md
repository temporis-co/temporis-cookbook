# Tiingo Example

Example crawler that streams Tiingo IEX market data into Temporis via the data source ingest API.

## Setup

Create `.env` and set:

```
TS_TOKEN=...
TIINGO_TOKEN=...
```

## Run

```
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python tiingo.py
```
