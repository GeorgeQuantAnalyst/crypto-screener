import logging

import pandas as pd

from crypto_screener.constants import SEPARATOR
from crypto_screener.service.rating_service import RatingService
from crypto_screener.service.statistics_service import StatisticService
from crypto_screener.utils import parse_last_date, parse_last_price, resample_to_weekly_ohlc


class CryptoBaseScreeningStep:
    TIME_FRAME_DAILY = "1d"
    LENGTH_DEFAULT = 200

    def __init__(self, data_downloader):
        self.data_downloader = data_downloader

    def process(self, assets: pd.DataFrame):
        result = pd.DataFrame()

        logging.info(SEPARATOR)
        logging.info("Start crypto base screening step")
        logging.info(SEPARATOR)

        btc_ohlc_daily = self.data_downloader.download_ohlc("BinanceSpot", "BTCUSDT", self.TIME_FRAME_DAILY,
                                                            self.LENGTH_DEFAULT)

        count_assets = assets.shape[0]
        for index, asset in assets.iterrows():
            logging.info("Process asset - {} ({}/{})".format(asset["ticker"], index + 1, count_assets))
            try:
                exchange = asset["exchange"]
                ohlc_daily = self.data_downloader.download_ohlc(exchange, asset["ticker"], self.TIME_FRAME_DAILY,
                                                                self.LENGTH_DEFAULT)
                ohlc_weekly = resample_to_weekly_ohlc(ohlc_daily)
                last_price = parse_last_price(ohlc_daily)

                asset["last_price"] = last_price
                asset["last_date"] = parse_last_date(ohlc_daily)
                asset["rsi_14"] = StatisticService.calculate_actual_rsi(ohlc_daily, 14)
                asset["sma_20"] = StatisticService.calculate_actual_sma(ohlc_daily, 20)
                asset["sma_50"] = StatisticService.calculate_actual_sma(ohlc_daily, 50)
                asset["sma_200"] = StatisticService.calculate_actual_sma(ohlc_daily, 200)
                asset["atr%_14W"] = StatisticService.calculate_actual_atr_percentage(ohlc_weekly,
                                                                                     14,
                                                                                     last_price)
                asset["btc_corr"] = StatisticService.calculate_correlation(ohlc_daily, btc_ohlc_daily, 14)

                asset["moving_averages_rating"] = RatingService.calculate_moving_averages_rating(asset)
                asset["oscillators_rating"] = RatingService.calculate_oscillators_rating(asset)
                asset["volatility_rating"] = RatingService.calculate_volatility_rating(asset)
                asset["swing_analysed"] = False
                asset["position_analysed"] = False
                asset["selected"] = False

                result = pd.concat([result, pd.DataFrame([asset])])
            except:
                logging.exception("Problem with base screening on coin {}".format(asset["ticker"]))
                result = pd.concat([result, pd.DataFrame([asset])])

        logging.info(SEPARATOR)
        logging.info("Finished crypto base screening step")
        logging.info(SEPARATOR)

        return result
