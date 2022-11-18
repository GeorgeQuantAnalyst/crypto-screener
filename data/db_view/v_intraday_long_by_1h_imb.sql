DROP VIEW IF EXISTS v_intraday_long_by_1h_imb;

CREATE VIEW v_intraday_long_by_1h_imb AS
SELECT
  bs.ticker,
  bs.exchange,
  bi.imb_buy_1h_date,
  bi.imb_buy_1h_price,
  bi."imb_buy_1h_distance%",
  bs.last_price,
  bs.last_date,
  bs."atr%_3D"
FROM
  base_screening bs,
  buyer_imbalances bi
WHERE
  bs.ticker = bi.ticker
  AND bs.exchange = bi.exchange
  AND bs."atr%_3D" > 0.10
  AND bi.imb_buy_1h_date < DATETIME('now', '-5 hour')
  AND bi."imb_buy_1h_distance%" < 0.1
  AND bs.ticker NOT IN (
    SELECT
      ticker
    FROM
      buyer_imbalances_processed bip
    WHERE
      bip.ticker = bi.ticker
      AND bip.imb_price = bi.imb_buy_1h_price
      AND bip.imb_date = bi.imb_buy_1h_date
  )
ORDER BY
  bi."imb_buy_1h_distance%" ASC;