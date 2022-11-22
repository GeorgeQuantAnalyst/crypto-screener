import math

import pandas as pd
import pandas_ta as ta


class StatisticService:

    @staticmethod
    def calculate_actual_rsi(ohlc: pd.DataFrame, length: int = 14) -> float:
        if length > ohlc.shape[0]:
            return None

        rsi = ohlc.ta.rsi(length=length)
        actual_rsi = rsi.tail(1).values[0]

        if math.isnan(actual_rsi):
            return None
        return round(actual_rsi, 6)

    @staticmethod
    def calculate_actual_sma(ohlc: pd.DataFrame, length: int = 20) -> float:
        if length > ohlc.shape[0]:
            return None

        sma = ohlc.ta.sma(length=length)
        actual_sma = sma.tail(1).values[0]

        if math.isnan(actual_sma):
            return None
        return round(actual_sma, 6)

    @staticmethod
    def calculate_actual_atr_percentage(ohlc: pd.DataFrame, length: int, last_price: int) -> float:
        if length > ohlc.shape[0]:
            return None

        atr = ohlc.ta.atr(length=length)
        actual_atr = atr.tail(1).values[0]

        if math.isnan(actual_atr):
            return None
        return round(actual_atr / last_price, 6)

    @staticmethod
    def calculate_correlation(ohlc: pd.DataFrame, ohlc_btc: pd.DataFrame, length: int) -> float:
        ohlc_btc_prepared = ohlc_btc.copy()

        if ohlc_btc_prepared.tail(1).index != ohlc.tail(1).index:
            ohlc_btc_prepared = ohlc_btc_prepared.drop(ohlc_btc_prepared.tail(1).index)  # drop last n rows

        correlation = ohlc.tail(length)["close"].corr(ohlc_btc_prepared.tail(length)["close"])
        return round(correlation, 2)
