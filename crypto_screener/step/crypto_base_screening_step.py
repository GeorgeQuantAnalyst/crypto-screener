import logging

import pandas as pd

from crypto_screener.constants import SEPARATOR
from crypto_screener.queries import SELECT_OHLC_ROWS
from crypto_screener.service.rating_service import RatingService
from crypto_screener.service.statistics_service import StatisticService
from crypto_screener.utils import parse_last_date, parse_last_price


class CryptoBaseScreeningStep:

    def __init__(self, crypto_history_db_conn, crypto_screener_db_conn):
        self.crypto_history_db_conn = crypto_history_db_conn
        self.crypto_screener_db_conn = crypto_screener_db_conn

    def process(self, assets: pd.DataFrame):
        result = pd.DataFrame()

        logging.info(SEPARATOR)
        logging.info("Start crypto base screening step")
        logging.info(SEPARATOR)

        btc_ohlc_daily = pd.read_sql_query(SELECT_OHLC_ROWS.format("BTCUSDT", "OkxSpot", "D"),
                                           con=self.crypto_history_db_conn, index_col="date", parse_dates=["date"])

        count_assets = assets.shape[0]
        for index, asset in assets.iterrows():
            try:
                ticker = asset["ticker"]
                exchange = asset["exchange"]
                logging.info("Process asset - {} ({}/{})".format(asset["ticker"], index + 1, count_assets))

                ohlc_daily = pd.read_sql_query(SELECT_OHLC_ROWS.format(ticker, exchange, "D"),
                                               con=self.crypto_history_db_conn, index_col="date", parse_dates=["date"])
                ohlc_weekly = pd.read_sql_query(SELECT_OHLC_ROWS.format(ticker, exchange, "W"),
                                                con=self.crypto_history_db_conn, index_col="date", parse_dates=["date"])

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

                result = pd.concat([result, pd.DataFrame([asset])])
            except:
                logging.exception("Problem with base screening on coin {}".format(asset["ticker"]))

        result.to_sql(name="base_screening", con=self.crypto_screener_db_conn, if_exists="replace", index=False)
        self.crypto_screener_db_conn.commit()

        logging.info(SEPARATOR)
        logging.info("Finished crypto base screening step")
        logging.info(SEPARATOR)
