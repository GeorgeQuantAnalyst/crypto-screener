import pandas as pd


class ImbalanceService:
    COUNT_SKIP_CANDLES = 4

    @staticmethod
    def find_buyer_imbalances(ohlc: pd.DataFrame) -> pd.DataFrame:
        ohlc_copy = ohlc.copy()
        ohlc_copy["date"] = ohlc_copy.index
        ohlc_copy.index = (range(ohlc_copy.shape[0]))

        # Mark green candles
        ohlc_copy["green"] = ohlc_copy["open"].lt(ohlc_copy["close"])

        # Decide 3 days consecutive green candles
        ohlc_copy['next_3days_green'] = ohlc_copy[::-1].rolling(3)['green'].sum().eq(3)

        # Find buyer imbalances
        buyer_imbalances = ImbalanceService.__find_buyer_imbalances(ohlc_copy)

        return buyer_imbalances

    @staticmethod
    def find_selling_imbalances(ohlc: pd.DataFrame) -> pd.DataFrame:
        ohlc_copy = ohlc.copy()
        ohlc_copy["date"] = ohlc_copy.index
        ohlc_copy.index = (range(ohlc_copy.shape[0]))

        # Mark red candles
        ohlc_copy["red"] = ohlc_copy["open"].gt(ohlc_copy["close"])

        # Decide 3 days consecutive red candles
        ohlc_copy['next_3days_red'] = ohlc_copy[::-1].rolling(3)['red'].sum().eq(3)

        # Find seller imbalances
        seller_imbalances = ImbalanceService.__find_seller_imbalances(ohlc_copy)

        return seller_imbalances

    @staticmethod
    def __find_buyer_imbalances(ohlc_copy: pd.DataFrame) -> pd.DataFrame:
        previous_open_price = 0
        is_previous_candle_3days_green = False

        buyer_imbalances = []

        for index, row in ohlc_copy.iterrows():
            if row["next_3days_green"] is True and is_previous_candle_3days_green is False:
                start_imbalance = previous_open_price
                is_tested_imbalance = start_imbalance > ohlc_copy[index + ImbalanceService.COUNT_SKIP_CANDLES:][
                    "low"].min()

                buyer_imbalances.append({
                    "date": row["date"],
                    "price": start_imbalance,
                    "tested": is_tested_imbalance
                })

            previous_open_price = row["open"]
            is_previous_candle_3days_green = row["next_3days_green"]

        return pd.DataFrame(buyer_imbalances)

    @staticmethod
    def __find_seller_imbalances(ohlc_copy: pd.DataFrame) -> pd.DataFrame:
        previous_open_price = 0
        is_previous_candle_3days_red = False

        seller_imbalances = []

        for index, row in ohlc_copy.iterrows():
            if row["next_3days_red"] is True and is_previous_candle_3days_red is False:
                start_imbalance = previous_open_price
                is_tested_imbalance = start_imbalance < ohlc_copy[index + ImbalanceService.COUNT_SKIP_CANDLES:][
                    "high"].max()

                seller_imbalances.append({
                    "date": row["date"],
                    "price": start_imbalance,
                    "tested": is_tested_imbalance
                })

            previous_open_price = row["open"]
            is_previous_candle_3days_red = row["next_3days_red"]

        return pd.DataFrame(seller_imbalances)
