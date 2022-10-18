SELECT
  bs.ticker,
  bs.exchange,
  bi.imb_buy_4h_date,
  bi.imb_buy_4h_price,
  bi."imb_buy_4h_distance%",
  bs.last_price,
  bs.last_date,
  bs.oscillators_rating_4h
FROM
  base_screening bs,
  buyer_imbalances bi
WHERE
  bs.ticker = bi.ticker
  AND bs.exchange = bi.exchange
  AND bs.exchange = 'PhemexFutures'
  AND bs.volatility_rating in ('MEDIUM', 'HIGH')
  AND bs.oscillators_rating_4h in ('BEARISH', 'OVERSOLD')
  AND bi."imb_buy_4h_distance%" < 0.2
  AND bi.imb_buy_4h_date < DATE('now', '-3 day')
  -- AND bs.ticker NOT IN (SELECT swing_long_analysed  FROM trader_data td WHERE swing_long_analysed  != "")
ORDER BY
  bi."imb_buy_4h_distance%" ASC