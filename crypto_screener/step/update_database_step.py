import logging
import sqlite3

import pandas as pd

from crypto_screener.constants import SEPARATOR


class UpdateDatabaseStep:

    def __init__(self, config):
        self.database_file_path = config["Steps"]["UpdateDatabaseStep"]["databaseFilePath"]
        self.base_screening_file_path = config["Steps"]["BaseScreeningStep"]["outputPath"]
        self.imbalance_buyer_screening_file_path = config["Steps"]["ImbalanceScreeningStep"]["outputBuyerImbalancePath"]
        self.imbalance_seller_screening_file_path = config["Steps"]["ImbalanceScreeningStep"][
            "outputSellerImbalancePath"]

    def process(self):
        logging.info(SEPARATOR)
        logging.info("Start update database step")
        logging.info(SEPARATOR)

        base_screening = pd.read_csv(self.base_screening_file_path)
        imbalance_buyer_screening = pd.read_csv(self.imbalance_buyer_screening_file_path)
        imbalance_seller_screening = pd.read_csv(self.imbalance_seller_screening_file_path)

        # TODO: update swing_analysed, position_analysed and selected from actual db
        # select ticker from xxx where swing_analysed = True

        conn = sqlite3.connect(self.database_file_path)
        logging.info("Update base_screening table")
        base_screening.to_sql(name="base_screening", con=conn, if_exists="replace")
        logging.info("Update buyer_imbalances table")
        imbalance_buyer_screening.to_sql(name="buyer_imbalances", con=conn, if_exists="replace")
        logging.info("Update seller_imbalances table")
        imbalance_seller_screening.to_sql(name="seller_imbalances", con=conn, if_exists="replace")
        conn.commit()
        conn.close()

        logging.info(SEPARATOR)
        logging.info("Finished update database step")
        logging.info(SEPARATOR)
