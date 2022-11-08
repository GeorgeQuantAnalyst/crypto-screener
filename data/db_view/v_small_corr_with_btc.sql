DROP VIEW IF EXISTS v_small_corr_with_btc;

CREATE VIEW v_small_corr_with_btc AS
SELECT
  *
FROM
  base_screening bs
WHERE
  btc_corr > -0.2 AND btc_corr < 0.2;