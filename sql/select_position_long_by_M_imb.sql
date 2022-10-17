 SELECT
  bs.ticker,
  bi.imb_buy_M_date,
  bi.imb_buy_M_price,
  bi."imb_buy_M_distance%",
  bs.last_price,
  bs.sma_20,
  bs.sma_50,
  bs.sma_200,
  bs.moving_averages_rating
FROM
  base_screening bs,
  buyer_imbalances bi
WHERE
  bs.ticker = bi.ticker
  AND bs.exchange = bi.exchange
  AND bs.exchange = 'OkxSpot'
  AND bs.last_price > 0.01
  AND bs.moving_averages_rating IN (
    'STRONG_DOWN_TREND', 'DOWN_TREND'
  )
  AND bi."imb_buy_M_distance%" < 0.5
  AND bi.imb_buy_W_date < DATE('now', '-150 day')
    -- AND bs.ticker NOT IN (SELECT position_long_analysed  FROM trader_data td WHERE position_long_analysed  != "")
ORDER BY
  bi."imb_buy_M_distance%"
