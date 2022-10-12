import logging
import time

import ccxt
import pandas as pd
from ccxt import RateLimitExceeded


class DataDownloader:

    def __init__(self, rate_exceed_delay_seconds):
        self.phemex_client = ccxt.phemex()
        self.kucoin_client = ccxt.kucoin()
        self.binance_client = ccxt.binance()
        self.okx_client = ccxt.okx()
        self.rate_exceed_delay_seconds = rate_exceed_delay_seconds

    def download_ohlc(self, exchange, ticker, time_frame="1d", length=200):
        logging.debug("Start downloading {} OHLC for {} on exchange {}".format(time_frame, ticker, exchange))
        match exchange:
            case "PhemexFutures":
                try:
                    # Faster download from other exchange if pair exist
                    return self.__download_ohlc(self.binance_client,
                                                ticker.replace("USDPERP", "USDT").replace("1", "").replace("0", ""),
                                                time_frame, length)
                except:
                    logging.info("Using slow download for {} on exchange - {}".format(ticker, exchange))
                    return self.__download_ohlc(self.phemex_client,
                                                ticker.replace("PERP", "").replace("100", "u100"),
                                                time_frame, length)
            case "KucoinSpot":
                return self.__download_ohlc(self.kucoin_client, ticker.replace("USDT", "-USDT"),
                                            time_frame, length)
            case "BinanceSpot":
                return self.__download_ohlc(self.binance_client, ticker, time_frame, length)

            case "OkxSpot":
                return self.__download_ohlc(self.okx_client, ticker.replace("USDT", "-USDT"),
                                            time_frame, length)
            case _:
                raise Exception("Not supported exchange - {}".format(exchange))

    def __download_ohlc(self, exchange_client, ticker, time_frame, length):
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
