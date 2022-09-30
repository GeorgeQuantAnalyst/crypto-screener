import logging
import time

import ccxt
import yfinance as yf
import pandas as pd
from ccxt import RateLimitExceeded


class DataDownloader:

    def __init__(self, rate_exceed_delay_seconds):
        self.phemex_client = ccxt.phemex()
        self.kucoin_client = ccxt.kucoin()
        self.binance_client = ccxt.binance()
        self.okx_client = ccxt.okx()
        self.rate_exceed_delay_seconds = rate_exceed_delay_seconds

    def download_daily_ohlc(self, exchange, ticker):
        logging.debug("Start downloading daily OHLC for {} on exchange {}".format(ticker, exchange))
        if exchange == "PhemexFutures":
            return self.__download_daily_ohlc_from_ccxt(self.phemex_client,
                                                        ticker.replace("PERP", "").replace("100", "u100"))

        if exchange == "KucoinSpot":
            return self.__download_daily_ohlc_from_ccxt(self.kucoin_client, ticker.replace("USDT", "-USDT"))

        if exchange == "BinanceSpot":
            return self.__download_daily_ohlc_from_ccxt(self.binance_client, ticker)

        if exchange == "OkxSpot":
            return self.__download_daily_ohlc_from_ccxt(self.okx_client, ticker.replace("USDT", "-USDT"))

        if exchange == "SimpleFx":
            return self.__download_daily_ohlc_from_yfinance(ticker)

        raise Exception("Not supported exchange - {}".format(exchange))

    def __download_daily_ohlc_from_crypto_data_download(self, exchange, ticker):
        ohlc_daily = pd.read_csv(self.CRYPTO_DATA_DOWNLOADER_URL.format(exchange, ticker),
                                 skiprows=1)
        ohlc_daily["date"] = pd.to_datetime(ohlc_daily["date"])
        ohlc_daily.set_index(["date"], inplace=True)
        ohlc_daily.sort_values(["date"], ascending=True, inplace=True)
        return ohlc_daily[["open", "high", "low", "close"]]

    def __download_daily_ohlc_from_ccxt(self, exchange_client, ticker):
        while True:
            try:
                ohlc_daily_raw = exchange_client.fetch_ohlcv(ticker, timeframe="1d", limit=200)
                ohlc_daily = pd.DataFrame(ohlc_daily_raw, columns=["date", "open", "high", "low", "close", "volume"])
                ohlc_daily["date"] = pd.to_datetime(ohlc_daily["date"], unit='ms')
                ohlc_daily.set_index(["date"], inplace=True)
                ohlc_daily.sort_values(["date"], ascending=True, inplace=True)
                return ohlc_daily
            except RateLimitExceeded:
                logging.warning(
                    "RateLimitExceeded: Too Many Requests on exchange api, app will be sleep {} seconds before recall api."
                    .format(self.rate_exceed_delay_seconds))
                time.sleep(self.rate_exceed_delay_seconds)

    def __download_daily_ohlc_from_yfinance(self, ticker):
        ohlc_daily_raw = yf.Ticker(ticker).history("200d","1d")
        ohlc_daily_raw.rename(columns={"Open":"open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
        return ohlc_daily_raw[["open","high","low","close","volume"]]
