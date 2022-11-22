import logging
from datetime import date, datetime, timedelta

import pandas as pd
import yaml


def load_config(file_path: str) -> dict:
    try:
        with open(file_path, 'r') as stream:
            return yaml.safe_load(stream)
    except FileNotFoundError:
        logging.exception("Problem with loading configuration from file, the application will be terminated.")
        exit(1)
    except yaml.YAMLError:
        logging.exception("Problem with parsing configuration, the application will be terminated.")
        exit(1)


def parse_last_price(ohlc: pd.DataFrame) -> float:
    return ohlc.tail(1)["close"].values[0]


def parse_last_date(ohlc: pd.DataFrame) -> datetime:
    return ohlc.tail(1).index.date[0].strftime("%d.%m.%Y")


def resample_to_weekly_ohlc(ohlc_daily: pd.DataFrame) -> pd.DataFrame:
    return ohlc_daily.resample("W").aggregate({'open': 'first',
                                               'high': 'max',
                                               'low': 'min',
                                               'close': 'last'})


def is_actual_data(ohlc_daily: pd.DataFrame) -> bool:
    last_date = parse_last_date(ohlc_daily)
    return datetime.strptime(last_date, "%d.%m.%Y").date() >= (date.today() - timedelta(days=1))
