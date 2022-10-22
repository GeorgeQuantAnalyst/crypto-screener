import logging

import pandas as pd

from crypto_screener.constants import SEPARATOR
from crypto_screener.service.data_downloader import DataDownloader
from crypto_screener.utils import is_actual_data, parse_last_date


class DataDownloadStep:

    def __init__(self, config, crypto_history_db_conn):
        self.crypto_history_db_conn = crypto_history_db_conn
        self.data_downloader = DataDownloader(config)

        self.four_hours_ohlc_history = config["steps"]["dataDownloadStep"]["fourHoursOhlcHistory"]
        self.daily_ohlc_history = config["steps"]["dataDownloadStep"]["dailyOhlcHistory"]
        self.weekly_ohlc_history = config["steps"]["dataDownloadStep"]["weeklyOhlcHistory"]
        self.monthly_ohlc_history = config["steps"]["dataDownloadStep"]["monthlyOhlcHistory"]

    def process(self, assets: pd.DataFrame):
        logging.info(SEPARATOR)
        logging.info("Start data download step")
        logging.info(SEPARATOR)

        count_assets = assets.shape[0]
        for index, asset in assets.iterrows():
            try:
                ticker = asset["ticker"]
                exchange = asset["exchange"]
                logging.info("Process asset - {} ({}/{})".format(ticker, index + 1, count_assets))

                ohlc_4h = self.data_downloader.download_ohlc(exchange, ticker, "4h",
                                                             self.four_hours_ohlc_history)
                ohlc_daily = self.data_downloader.download_ohlc(exchange, ticker, "1d",
                                                                self.daily_ohlc_history)
                ohlc_weekly = self.data_downloader.download_ohlc(exchange, ticker, "1w",
                                                                 self.weekly_ohlc_history)
                ohlc_monthly = self.data_downloader.download_ohlc(exchange, ticker, "1M",
                                                                  self.monthly_ohlc_history)

                if not is_actual_data(ohlc_daily):
                    msg = "Not download actual OHLC data for ticker {} - {}, last date is: {}. Please contact your data-provider..."
                    logging.warning(msg.format(ticker, exchange, parse_last_date(ohlc_daily)))

                ohlc_4h.to_sql(name="{}_{}_4h".format(ticker, exchange), con=self.crypto_history_db_conn,
                               if_exists="replace")

                ohlc_daily.to_sql(name="{}_{}_D".format(ticker, exchange), con=self.crypto_history_db_conn,
                                  if_exists="replace")

                ohlc_weekly.to_sql(name="{}_{}_W".format(ticker, exchange), con=self.crypto_history_db_conn,
                                   if_exists="replace")

                ohlc_monthly.to_sql(name="{}_{}_M".format(ticker, exchange), con=self.crypto_history_db_conn,
                                    if_exists="replace")
            except:
                logging.exception("Problem with data download on coin {}".format(asset["ticker"]))

        self.crypto_history_db_conn.commit()
        logging.info(SEPARATOR)
        logging.info("Finished data download step")
        logging.info(SEPARATOR)
