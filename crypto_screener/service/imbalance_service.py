import pandas as pd


class ImbalanceService:

    @staticmethod
    def find_first_buyer_imbalance(ohlc: pd.DataFrame):
        ohlc_copy = ohlc.copy()

        # Mark green candles
        ohlc_copy["green"] = ohlc_copy["open"].lt(ohlc["close"])

        # Decide 3 days consecutive green candles
        ohlc_copy['next_3days_green'] = ohlc_copy[::-1].rolling(3)['green'].sum().eq(3)

        # Find buyer imbalances
        buyer_imbalances = ImbalanceService.__find_buyer_imbalances(ohlc_copy)

        first_buyer_imbalance = buyer_imbalances.loc[not buyer_imbalances["is_tested"]].head(1)

        return first_buyer_imbalance

    @staticmethod
    def find_first_selling_imbalance(ohlc):
        ohlc_copy = ohlc.copy()

        # Mark red candles
        ohlc_copy["red"] = ohlc_copy["open"].gt(ohlc_copy["close"])

        # Decide 3 days consecutive red candles
        ohlc_copy['next_3days_red'] = ohlc_copy[::-1].rolling(3)['red'].sum().eq(3)

        # Find seller imbalances
        seller_imbalances = ImbalanceService.__find_seller_imbalances(ohlc_copy)

        first_seller_imbalance = seller_imbalances.loc[not seller_imbalances["is_tested"]].head(1)

        return first_seller_imbalance

    @staticmethod
    def __find_buyer_imbalances(ohlc_copy):
        previous_open_price = 0
        is_previous_candle_3days_green = False

        buyer_imbalances = pd.DataFrame()

        for index, row in ohlc_copy.iterrows():
            if row["next_3days_green"] == True and is_previous_candle_3days_green == False:
                start_imbalance = previous_open_price
                is_tested_imbalance = start_imbalance > ohlc_copy[index + 3:]["low"].min()

                imbalance_row = pd.DataFrame({
                    "imbalance_date": [row["date"]],
                    "imbalace_price": [start_imbalance],
                    "is_tested": [is_tested_imbalance]
                })
                buyer_imbalances = pd.concat([buyer_imbalances, imbalance_row], ignore_index=True)

            previous_open_price = row["open"]
            is_previous_candle_3days_green = row["next_3days_green"]

        return buyer_imbalances

    @staticmethod
    def __find_seller_imbalances(ohlc_copy):
        previous_open_price = 0
        is_previous_candle_3days_red = False

        seller_imbalances = pd.DataFrame()

        for index, row in ohlc_copy.iterrows():
            if row["next_3days_red"] == True and is_previous_candle_3days_red == False:
                start_imbalance = previous_open_price
                is_tested_imbalance = start_imbalance < ohlc_copy[index + 3:]["high"].max()

                imbalance_row = pd.DataFrame({
                    "imbalance_date": [row["date"]],
                    "imbalace_price": [start_imbalance],
                    "is_tested": [is_tested_imbalance]
                })
                seller_imbalances = pd.concat([seller_imbalances, imbalance_row], ignore_index=True)

            previous_open_price = row["open"]
            is_previous_candle_3days_red = row["next_3days_red"]

        return seller_imbalances
