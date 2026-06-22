# Interactive Brokers

Example crawler that streams Interactive brokers market data into Temporis via the data source ingest API.


[API Reference](https://www.interactivebrokers.com/campus/ibkr-api-page/cpapi-v1)

[Availability schedule](https://www.interactivebrokers.com/en/software/systemStatus.php)

## Create a virtual environment
```
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Run the server
```
uvicorn server:app --host 0.0.0.0 --port 6000
```
