SELECT
  bs.ticker,
  bs.exchange,
  bi.imb_buy_D_date,
  bi.imb_buy_D_price,
  bi."imb_buy_D_distance%",
  bs.last_price,
  bs.last_date,
  bs.oscillators_rating_D
FROM
  base_screening bs,
  buyer_imbalances bi
WHERE
  bs.ticker = bi.ticker
  AND bs.exchange = bi.exchange
  AND bs.exchange = 'PhemexFutures'
  AND bs.volatility_rating in ('MEDIUM', 'HIGH')
  AND bs.oscillators_rating_D in ('BEARISH', 'OVERSOLD')
  AND bi."imb_buy_D_distance%" < 0.2
  AND bi.imb_buy_D_date < DATE('now', '-5 day')
  AND bs.ticker NOT IN (
    SELECT
      ticker
    FROM
      buyer_imbalances_processed bip
    WHERE
      bip.ticker = bi.ticker
      AND bip.imb_price = bi.imb_buy_D_price
      AND bip.imb_date = bi.imb_buy_D_date
  )
ORDER BY
  bi."imb_buy_D_distance%" ASC