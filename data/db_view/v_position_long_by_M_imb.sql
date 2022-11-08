DROP VIEW IF EXISTS v_position_long_by_M_imb;

CREATE VIEW v_position_long_by_M_imb AS
SELECT
  bs.ticker,
  bs.exchange,
  bi.imb_buy_M_date,
  bi.imb_buy_M_price,
  bi."imb_buy_M_distance%",
  bs.last_price,
  bs.last_date,
  bs.moving_averages_rating
FROM
  base_screening bs,
  buyer_imbalances bi
WHERE
  bs.ticker = bi.ticker
  AND bs.exchange = bi.exchange
  AND bs.exchange = 'PhemexFutures'
  AND moving_averages_rating IN ('DOWN_TREND', 'STRONG_DOWN_TREND')
  AND bi."imb_buy_M_distance%" < 0.5
  AND bi.imb_buy_M_date < DATE('now', '-150 day')
  AND bs.ticker NOT IN (
    SELECT
      ticker
    FROM
      buyer_imbalances_processed bip
    WHERE
      bip.ticker = bi.ticker
      AND bip.imb_price = bi.imb_buy_M_price
      AND bip.imb_date = bi.imb_buy_M_date
  )
ORDER BY
  bi."imb_buy_M_distance%" ASC;