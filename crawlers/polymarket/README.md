# Polymarket Example

Example crawler that streams Polymarket Hourly Crypto market data into Temporis via the data source ingest API.

## Setup

Create `.env` and set:

```
TS_TOKEN=...
```

## Run
```
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python polymarket.py
```
