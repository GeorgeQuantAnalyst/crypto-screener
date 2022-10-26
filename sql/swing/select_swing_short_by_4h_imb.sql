SELECT
  bs.ticker,
  bs.exchange,
  si.imb_sell_4h_date,
  si.imb_sell_4h_price,
  si."imb_sell_4h_distance%",
  bs.last_price,
  bs.last_date,
  bs.oscillators_rating_4h
FROM
  base_screening bs,
  seller_imbalances si
WHERE
  bs.ticker = si.ticker
  AND bs.exchange = si.exchange
  AND bs.exchange = 'PhemexFutures'
  AND bs.volatility_rating in ('MEDIUM', 'HIGH')
  AND bs.oscillators_rating_4h in ('BULLISH', 'OVERBOUGHT')
  AND si."imb_sell_4h_distance%" < 0.2
  AND si.imb_sell_4h_date < DATE('now', '-1 day')
  AND bs.ticker NOT IN (
    SELECT
      ticker
    FROM
      seller_imbalances_processed sip
    WHERE
      sip.ticker = si.ticker
      AND sip.imb_price = si.imb_sell_4h_price
      AND sip.imb_date = si.imb_sell_4h_date
  )
ORDER BY
  si."imb_sell_4h_distance%" ASC