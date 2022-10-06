import logging
import time

import ccxt
import pandas as pd
import yfinance as yf
from ccxt import RateLimitExceeded


class DataDownloader:

    def __init__(self, rate_exceed_delay_seconds):
        self.phemex_client = ccxt.phemex()
        self.kucoin_client = ccxt.kucoin()
        self.binance_client = ccxt.binance()
        self.okx_client = ccxt.okx()
        self.rate_exceed_delay_seconds = rate_exceed_delay_seconds

    def download_daily_ohlc(self, exchange, ticker):
        return self.download_ohlc(exchange, ticker, "1d", 200)

    def download_ohlc(self, exchange, ticker, time_frame, length):
        logging.debug("Start downloading daily OHLC for {} on exchange {}".format(ticker, exchange))
        if exchange == "PhemexFutures":
            return self.__download_daily_ohlc_from_ccxt(self.phemex_client,
                                                        ticker.replace("PERP", "").replace("100", "u100"),
                                                        time_frame, length)

        if exchange == "KucoinSpot":
            return self.__download_daily_ohlc_from_ccxt(self.kucoin_client, ticker.replace("USDT", "-USDT"),
                                                        time_frame, length)

        if exchange == "BinanceSpot":
            return self.__download_daily_ohlc_from_ccxt(self.binance_client, ticker, time_frame, length)

        if exchange == "OkxSpot":
            return self.__download_daily_ohlc_from_ccxt(self.okx_client, ticker.replace("USDT", "-USDT"),
                                                        time_frame, length)

        if exchange == "SimpleFx":
            return self.__download_daily_ohlc_from_yfinance(ticker, time_frame, length)

        raise Exception("Not supported exchange - {}".format(exchange))

    def __download_daily_ohlc_from_crypto_data_download(self, exchange, ticker):
        ohlc_daily = pd.read_csv(self.CRYPTO_DATA_DOWNLOADER_URL.format(exchange, ticker),
                                 skiprows=1)
        ohlc_daily["date"] = pd.to_datetime(ohlc_daily["date"])
        ohlc_daily.set_index(["date"], inplace=True)
        ohlc_daily.sort_values(["date"], ascending=True, inplace=True)
        return ohlc_daily[["open", "high", "low", "close"]]

    def __download_daily_ohlc_from_ccxt(self, exchange_client, ticker, time_frame, length):
        while True:
            try:
                ohlc_daily_raw = exchange_client.fetch_ohlcv(ticker, timeframe=time_frame, limit=length)
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

    def __download_daily_ohlc_from_yfinance(self, ticker, time_frame, length):
        time_frame_formatted = time_frame
        length_formatted = "{}d".format(length)
        if time_frame == "1d":
            time_frame_formatted = "1d"
            length_formatted = "{}d".format(length)

        if time_frame == "1w":
            time_frame_formatted = "1wk"
            length_formatted = "{}wk".format(length)

        if time_frame == "1M":
            time_frame_formatted = "1mo"
            length_formatted = "mo".format(length)

        ohlc_daily_raw = yf.Ticker(ticker).history(length_formatted, time_frame_formatted)
        ohlc_daily_raw.rename(
            columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
        return ohlc_daily_raw[["open", "high", "low", "close", "volume"]]
