DROP VIEW IF EXISTS v_position_short_by_W_imb;

CREATE VIEW v_position_short_by_W_imb AS
SELECT
  bs.ticker,
  bs.exchange,
  si.imb_sell_W_date,
  si.imb_sell_W_price,
  si."imb_sell_W_distance%",
  bs.last_price,
  bs.last_date,
  bs.moving_averages_rating
FROM
  base_screening bs,
  seller_imbalances si
WHERE
  bs.ticker = si.ticker
  AND bs.exchange = si.exchange
  AND bs.exchange = 'PhemexFutures'
  AND bs.moving_averages_rating IN ('UP_TREND', 'STRONG_UP_TREND')
  AND si."imb_sell_W_distance%" < 0.5
  AND si.imb_sell_W_date < DATE('now', '-35 day')
  AND bs.ticker NOT IN (
    SELECT
      ticker
    FROM
      seller_imbalances_processed sip
    WHERE
      sip.ticker = si.ticker
      AND sip.imb_price = si.imb_sell_W_price
      AND sip.imb_date = si.imb_sell_W_date
  )
ORDER BY
  si."imb_sell_W_distance%" ASC;