DROP VIEW IF EXISTS v_position_long_by_W_imb;

CREATE VIEW v_position_long_by_W_imb AS
SELECT
  bs.ticker,
  bs.exchange,
  bi.imb_buy_W_date,
  bi.imb_buy_W_price,
  bi."imb_buy_W_distance%",
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
  AND bs.moving_averages_rating IN ('DOWN_TREND', 'STRONG_DOWN_TREND')
  AND bi."imb_buy_W_distance%" < 0.5
  AND bi.imb_buy_W_date < DATE('now', '-35 day')
  AND bs.ticker NOT IN (
    SELECT
      ticker
    FROM
      buyer_imbalances_processed bip
    WHERE
      bip.ticker = bi.ticker
      AND bip.imb_price = bi.imb_buy_W_price
      AND bip.imb_date = bi.imb_buy_W_date
  )
ORDER BY
  bi."imb_buy_W_distance%" ASC;