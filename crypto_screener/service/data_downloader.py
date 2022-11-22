import logging
import time

import ccxt
import pandas as pd
from ccxt import RateLimitExceeded, Exchange


class DataDownloader:

    def __init__(self, config: dict):
        self.phemex_client = ccxt.phemex()
        self.kucoin_client = ccxt.kucoin()
        self.binance_client = ccxt.binance()
        self.okx_client = ccxt.okx()
        self.rate_exceed_delay_seconds = config["services"]["dataDownloader"]["rateExceedDelaySeconds"]
        self.binance_not_supported_pairs = config["services"]["dataDownloader"]["binanceNotSupportedPairs"]

    def download_ohlc(self, exchange: str, ticker: str, time_frame: str = "1d", length: int = 200) -> pd.DataFrame:
        logging.debug("Start downloading {} OHLC for {} on exchange {}".format(time_frame, ticker, exchange))
        match exchange:
            case "PhemexFutures":
                try:
                    # Faster download from other exchange if pair exist
                    ticker_prepared = ticker.replace("USD", "USDT").replace("u1", "").replace("0", "")
                    if ticker_prepared in self.binance_not_supported_pairs:
                        raise Exception("Binance not supported pair - {}".format(ticker_prepared))
                    return self.__download_ohlc(self.binance_client, ticker_prepared, time_frame, length)
                except:
                    logging.info("Using slow download for {} on exchange - {}".format(ticker, exchange))
                    return self.__download_ohlc(self.phemex_client, ticker, time_frame, length)
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

    def __download_ohlc(self, exchange_client: Exchange, ticker: str, time_frame: str, length: str) -> pd.DataFrame:
        while True:
            try:
                ohlc_raw = exchange_client.fetch_ohlcv(ticker, timeframe=time_frame, limit=length)
                ohlc = pd.DataFrame(ohlc_raw, columns=["date", "open", "high", "low", "close", "volume"])
                ohlc["date"] = pd.to_datetime(ohlc["date"], unit='ms')
                ohlc.set_index(["date"], inplace=True)
                ohlc.sort_values(["date"], ascending=True, inplace=True)
                return ohlc
            except RateLimitExceeded:
                logging.warning(
                    "RateLimitExceeded: Too Many Requests on exchange api, app will be sleep {} seconds before recall api."
                    .format(self.rate_exceed_delay_seconds))
                time.sleep(self.rate_exceed_delay_seconds)
