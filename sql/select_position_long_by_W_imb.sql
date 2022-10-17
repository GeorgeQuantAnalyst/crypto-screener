SELECT
  bs.ticker,
  bi.imb_buy_W_date,
  bi.imb_buy_W_price,
  bi."imb_buy_W_distance%",
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
  and bs.exchange = bi.exchange
  AND bs.exchange = 'OkxSpot'
  AND bs.last_price > 0.01
  AND bs.moving_averages_rating IN (
    'STRONG_DOWN_TREND', 'DOWN_TREND'
  )
  and bi."imb_buy_W_distance%" > 0
  and bi."imb_buy_W_distance%" < 0.5
order BY
  bi."imb_buy_W_distance%"
