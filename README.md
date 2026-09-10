# Temporis Cookbook
Examples and guides for using the [Temporis API](https://docs.temporis.co/)

## Crawlers

Each crawler pulls market data from a vendor and streams it into Temporis via the data source ingest API.

- [dukascopy](crawlers/dukascopy) - Forex bid/ask quotes over FIX 4.4
- [ibkr](crawlers/ibkr) - US equity bid/ask quotes via the Interactive Brokers Client Portal API
- [polymarket](crawlers/polymarket) - Hourly crypto Up/Down market best bid/ask via the Polymarket CLOB websocket
- [tiingo](crawlers/tiingo) - US equity trades via the Tiingo IEX websocket

## License

MIT License
