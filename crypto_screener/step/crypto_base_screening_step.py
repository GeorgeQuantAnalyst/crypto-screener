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
            logging.info("Process asset - {} ({}/{})".format(asset["Asset"], index + 1, count_assets))
            try:
                exchange = asset["Exchange"]
                ohlc_daily = self.data_downloader.download_ohlc(exchange, asset["Asset"], self.TIME_FRAME_DAILY,
                                                                self.LENGTH_DEFAULT)
                ohlc_weekly = resample_to_weekly_ohlc(ohlc_daily)
                last_price = parse_last_price(ohlc_daily)

                asset["LastPrice"] = last_price
                asset["LastDate"] = parse_last_date(ohlc_daily)
                asset["RSI_14"] = StatisticService.calculate_actual_rsi(ohlc_daily, 14)
                asset["SMA_20"] = StatisticService.calculate_actual_sma(ohlc_daily, 20)
                asset["SMA_50"] = StatisticService.calculate_actual_sma(ohlc_daily, 50)
                asset["SMA_200"] = StatisticService.calculate_actual_sma(ohlc_daily, 200)
                asset["ATR%_14W"] = StatisticService.calculate_actual_atr_percentage(ohlc_weekly,
                                                                                     14,
                                                                                     last_price)
                asset["BTC corr"] = StatisticService.calculate_correlation(ohlc_daily, btc_ohlc_daily, 14)

                asset["MovingAveragesRating"] = RatingService.calculate_moving_averages_rating(asset)
                asset["OscillatorsRating"] = RatingService.calculate_oscillators_rating(asset)
                asset["VolatilityRating"] = RatingService.calculate_volatility_rating(asset)

                result = pd.concat([result, pd.DataFrame([asset])])
            except:
                logging.exception("Problem with base screening on coin {}".format(asset["Asset"]))
                result = pd.concat([result, pd.DataFrame([asset])])

        logging.info(SEPARATOR)
        logging.info("Finished crypto base screening step")
        logging.info(SEPARATOR)

        return result

