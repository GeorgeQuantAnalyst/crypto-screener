import logging.config
import sqlite3

import pandas as pd

from crypto_screener.constants import LOGGER_CONFIG_FILE_PATH, CONFIG_FILE_PATH, __logo__
from crypto_screener.step.create_views_step import CreateViewsStep
from crypto_screener.step.crypto_base_screening_step import CryptoBaseScreeningStep
from crypto_screener.step.crypto_imbalance_screening_step import CryptoImbalanceScreeningStep
from crypto_screener.step.data_download_step import DataDownloadStep
from crypto_screener.step.load_processed_imbalances_step import LoadProcessedImbalancesStep
from crypto_screener.utils import load_config

if __name__ == "__main__":
    logging.config.fileConfig(fname=LOGGER_CONFIG_FILE_PATH, disable_existing_loggers=False)
    logging.info(__logo__)
    config = load_config(CONFIG_FILE_PATH)

    try:
        crypto_history_db_conn = sqlite3.connect(config["base"]["cryptoHistoryDbPath"])
        crypto_screener_db_conn = sqlite3.connect(config["base"]["cryptoScreenerDbPath"])

        data_download_step = DataDownloadStep(config, crypto_history_db_conn)
        base_screening_step = CryptoBaseScreeningStep(crypto_history_db_conn, crypto_screener_db_conn)
        imbalance_screening_step = CryptoImbalanceScreeningStep(crypto_history_db_conn, crypto_screener_db_conn)
        load_processed_imbalances_step = LoadProcessedImbalancesStep(config, crypto_screener_db_conn)
        create_views_step = CreateViewsStep(config, crypto_screener_db_conn)

        assets = pd.read_csv(config["base"]["assetsPath"])

        if config["steps"]["dataDownloadStep"]["enable"]:
            data_download_step.process(assets)

        if config["steps"]["baseScreeningStep"]["enable"]:
            base_screening_step.process(assets)

        if config["steps"]["imbalanceScreeningStep"]["enable"]:
            imbalance_screening_step.process(assets)

        if config["steps"]["loadProcessedImbalancesStep"]["enable"]:
            load_processed_imbalances_step.process()

        if config["steps"]["createViewsStep"]["enable"]:
            create_views_step.process()

        crypto_history_db_conn.close()
        crypto_screener_db_conn.close()
    except:
        logging.exception("Problem in application crypto-screener:")
