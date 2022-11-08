DROP VIEW IF EXISTS v_uptrend_markets;

CREATE VIEW v_uptrend_markets AS
SELECT
  ticker,
  last_price,
  sma_20,
  sma_50,
  sma_200,
  moving_averages_rating
FROM
  base_screening bs
WHERE
  moving_averages_rating IN ('STRONG_UP_TREND', 'UP_TREND')
  AND last_price > 0.01
order BY
  moving_averages_rating;