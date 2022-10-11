import logging

import pandas as pd

from crypto_screener.constants import SEPARATOR
from crypto_screener.queries import SELECT_OHLC_ROWS
from crypto_screener.service.imbalance_service import ImbalanceService
from crypto_screener.utils import parse_last_date, parse_last_price


class CryptoImbalanceScreeningStep:

    def __init__(self, crypto_history_db_conn, crypto_screener_db_conn):
        self.crypto_history_db_conn = crypto_history_db_conn
        self.crypto_screener_db_conn = crypto_screener_db_conn
        self.imbalance_service = ImbalanceService()

    def process(self, assets: pd.DataFrame):
        result_buyer_imbalances = pd.DataFrame()
        result_seller_imbalances = pd.DataFrame()

        logging.info(SEPARATOR)
        logging.info("Start crypto imbalance screening step")
        logging.info(SEPARATOR)

        count_assets = assets.shape[0]

        for index, asset in assets.iterrows():
            try:
                ticker = asset["ticker"]
                exchange = asset["exchange"]
                logging.info("Process asset - {} ({}/{})".format(ticker, index + 1, count_assets))

                ohlc_4h = pd.read_sql_query(SELECT_OHLC_ROWS.format(ticker, exchange, "4h"),
                                            con=self.crypto_history_db_conn, index_col="date", parse_dates=["date"])
                ohlc_daily = pd.read_sql_query(SELECT_OHLC_ROWS.format(ticker, exchange, "D"),
                                               con=self.crypto_history_db_conn, index_col="date", parse_dates=["date"])
                ohlc_weekly = pd.read_sql_query(SELECT_OHLC_ROWS.format(ticker, exchange, "W"),
                                                con=self.crypto_history_db_conn, index_col="date", parse_dates=["date"])
                ohlc_monthly = pd.read_sql_query(SELECT_OHLC_ROWS.format(ticker, exchange, "M"),
                                                 con=self.crypto_history_db_conn, index_col="date",
                                                 parse_dates=["date"])

                last_price = parse_last_price(ohlc_daily)
                asset["last_price"] = last_price
                asset["last_date"] = parse_last_date(ohlc_daily)

                asset_with_buyer_imbalances = asset.copy()
                self.append_first_buyer_untested_imbalance(asset_with_buyer_imbalances, last_price, "M", ohlc_monthly)
                self.append_first_buyer_untested_imbalance(asset_with_buyer_imbalances, last_price, "W", ohlc_weekly)
                self.append_first_buyer_untested_imbalance(asset_with_buyer_imbalances, last_price, "D", ohlc_daily)
                self.append_first_buyer_untested_imbalance(asset_with_buyer_imbalances, last_price, "4h", ohlc_4h)

                asset_with_seller_imbalances = asset.copy()
                self.append_first_seller_untested_imbalance(asset_with_seller_imbalances, last_price, "M", ohlc_monthly)
                self.append_first_seller_untested_imbalance(asset_with_seller_imbalances, last_price, "W", ohlc_weekly)
                self.append_first_seller_untested_imbalance(asset_with_seller_imbalances, last_price, "D", ohlc_daily)
                self.append_first_seller_untested_imbalance(asset_with_seller_imbalances, last_price, "4h", ohlc_4h)

                result_buyer_imbalances = pd.concat(
                    [result_buyer_imbalances, pd.DataFrame([asset_with_buyer_imbalances])])
                result_seller_imbalances = pd.concat(
                    [result_seller_imbalances, pd.DataFrame([asset_with_seller_imbalances])])
            except:
                logging.exception("Problem with compute imbalance on coin {}".format(asset["ticker"]))

        result_buyer_imbalances.to_sql(name="buyer_imbalances", con=self.crypto_screener_db_conn, if_exists="replace")
        result_seller_imbalances.to_sql(name="seller_imbalances", con=self.crypto_screener_db_conn, if_exists="replace")
        self.crypto_screener_db_conn.commit()

        logging.info(SEPARATOR)
        logging.info("Finished crypto imbalance screening step")
        logging.info(SEPARATOR)

    def append_first_buyer_untested_imbalance(self, asset, last_price, time_frame, ohlc):
        buyer_imbalances = self.imbalance_service.find_buyer_imbalances(ohlc)

        if buyer_imbalances.empty:
            return

        first_buyer_untested_imbalance = buyer_imbalances.loc[buyer_imbalances["tested"] == False].tail(1)

        if first_buyer_untested_imbalance.empty:
            return

        imbalance_price = first_buyer_untested_imbalance["price"].values[0]
        asset["imb_buy_{}_date".format(time_frame)] = pd.Timestamp(first_buyer_untested_imbalance["date"].values[0])
        asset["imb_buy_{}_price".format(time_frame)] = imbalance_price
        asset["imb_buy_{}_distance%".format(time_frame)] = round(1 - (imbalance_price / last_price), 2)

    def append_first_seller_untested_imbalance(self, asset, last_price, time_frame, ohlc):
        seller_imbalances = self.imbalance_service.find_selling_imbalances(ohlc)

        if seller_imbalances.empty:
            return

        first_seller_untested_imbalance = seller_imbalances.loc[seller_imbalances["tested"] == False].tail(1)

        if first_seller_untested_imbalance.empty:
            return

        imbalance_price = first_seller_untested_imbalance["price"].values[0]
        asset["imb_sell_{}_date".format(time_frame)] = pd.Timestamp(first_seller_untested_imbalance["date"].values[0])
        asset["imb_sell_{}_price".format(time_frame)] = imbalance_price
        asset["imb_sell_{}_distance%".format(time_frame)] = round(1 - (imbalance_price / last_price), 2) * -1
