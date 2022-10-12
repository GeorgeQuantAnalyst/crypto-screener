SELECT
  *
FROM
  base_screening bs
WHERE
  exchange IN ('PhemexFutures', 'OkxSpot')
  AND btc_corr > -0.2 AND btc_corr < 0.2