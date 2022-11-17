DROP VIEW IF EXISTS v_intraday_short_by_1h_imb;

CREATE VIEW v_intraday_short_by_1h_imb AS
SELECT
  bs.ticker,
  bs.exchange,
  si.imb_sell_1h_date,
  si.imb_sell_1h_price,
  si."imb_sell_1h_distance%",
  bs.last_price,
  bs.last_date,
  bs."atr%_3D"
FROM
  base_screening bs,
  seller_imbalances si
WHERE
  bs.ticker = si.ticker
  AND bs.exchange = si.exchange
  AND bs."atr%_3D" > 0.10
  AND si.imb_sell_1h_date < DATETIME('now', '-5 hour')
  AND bi."imb_sell_1h_distance%" < 0.1
  AND bs.ticker NOT IN (
    SELECT
      ticker
    FROM
      seller_imbalances_processed sip
    WHERE
      sip.ticker = si.ticker
      AND sip.imb_price = si.imb_sell_1h_price
      AND sip.imb_date = si.imb_sell_1h_date
  )
ORDER BY
  bi."imb_sell_1h_distance%" ASC;
