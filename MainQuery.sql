WITH period AS (
  SELECT
    (EXTRACT(EPOCH FROM NOW() - INTERVAL '7 days') * 1000)::bigint AS ts_min,
    (EXTRACT(EPOCH FROM NOW() - INTERVAL '24 hours') * 1000)::bigint AS ts_now,
    (EXTRACT(EPOCH FROM NOW() - INTERVAL '7 days 24 hours') * 1000)::bigint AS ts_wow
)
SELECT
  pa.merchant_id,
  pa.order_count_7d,
  pa.order_count_wow,
  pa.order_count_now,
  pa.order_count_global,
  ola.order_line_count_7d,
  ola.order_line_count_wow,
  ola.order_line_count_now,
  ola.order_line_count_global,
  cr.return_count_7d,
  cr.return_count_wow,
  cr.return_count_now,
  cr.return_count_global,
  pa.order_value_7d,
  pa.order_value_wow,
  pa.order_value_now,
  pa.order_value_global,
  cr.return_value_7d,
  cr.return_value_wow,
  cr.return_value_now,
  cr.return_value_global,
  cr.approve_count_7d,
  cr.approve_count_wow,
  cr.approve_count_now,
  cr.approve_count_global,
  cr.decline_count_7d,
  cr.decline_count_wow,
  cr.decline_count_now,
  cr.decline_count_global,
  COALESCE(cr.return_value_7d, 0) / NULLIF(pa.order_value_7d, 0) AS return_rate_7d,
  COALESCE(cr.return_value_wow, 0) / NULLIF(pa.order_value_wow, 0) AS return_rate_wow,
  COALESCE(cr.return_value_now, 0) / NULLIF(pa.order_value_now, 0) AS return_rate_now,
  COALESCE(cr.return_value_global, 0) / NULLIF(pa.order_value_global, 0) AS return_rate_global,
  pa.review_count_7d,
  pa.review_count_wow,
  pa.review_count_now,
  pa.review_count_global,
  pa.review_count_7d::numeric / NULLIF(pa.order_count_7d, 0) AS review_rate_7d,
  pa.review_count_wow::numeric / NULLIF(pa.order_count_wow, 0) AS review_rate_wow,
  pa.review_count_now::numeric / NULLIF(pa.order_count_now, 0) AS review_rate_now,
  pa.review_count_global::numeric / NULLIF(pa.order_count_global, 0) AS review_rate_global
FROM (
  SELECT
    merchant_id,
    COUNT(*) FILTER (WHERE timestamp >= (SELECT ts_min FROM period)) AS order_count_7d,
    COUNT(*) FILTER (WHERE timestamp >= (SELECT ts_wow FROM period)) AS order_count_wow,
    COUNT(*) FILTER (WHERE timestamp >= (SELECT ts_now FROM period)) AS order_count_now,
    COUNT(*) AS order_count_global,
    SUM(amount) FILTER (WHERE timestamp >= (SELECT ts_min FROM period)) AS order_value_7d,
    SUM(amount) FILTER (WHERE timestamp >= (SELECT ts_wow FROM period)) AS order_value_wow,
    SUM(amount) FILTER (WHERE timestamp >= (SELECT ts_now FROM period)) AS order_value_now,
    SUM(amount) AS order_value_global,
    COUNT(*) FILTER (WHERE timestamp >= (SELECT ts_min FROM period) AND decision = 'REVIEW') AS review_count_7d,
    COUNT(*) FILTER (WHERE timestamp >= (SELECT ts_wow FROM period) AND decision = 'REVIEW') AS review_count_wow,
    COUNT(*) FILTER (WHERE timestamp >= (SELECT ts_now FROM period) AND decision = 'REVIEW') AS review_count_now,
    COUNT(*) FILTER (WHERE decision = 'REVIEW') AS review_count_global,
    COUNT(DISTINCT customer_id) FILTER (WHERE timestamp >= (SELECT ts_min FROM period)) AS unique_customers_7d,
    COUNT(DISTINCT customer_id) FILTER (WHERE timestamp >= (SELECT ts_wow FROM period)) AS unique_customers_wow,
    COUNT(DISTINCT customer_id) FILTER (WHERE timestamp >= (SELECT ts_now FROM period)) AS unique_customers_now,
    COUNT(DISTINCT customer_id) AS unique_customers_global
  FROM purchase
  WHERE merchant_id = 13927
  GROUP BY merchant_id
) pa
JOIN (
  SELECT
    co.merchant_id,
    COUNT(DISTINCT co.id) FILTER (WHERE p.timestamp >= (SELECT ts_min FROM period)) AS order_line_count_7d,
    COUNT(DISTINCT co.id) FILTER (WHERE p.timestamp >= (SELECT ts_wow FROM period)) AS order_line_count_wow,
    COUNT(DISTINCT co.id) FILTER (WHERE p.timestamp >= (SELECT ts_now FROM period)) AS order_line_count_now,
    COUNT(DISTINCT co.id) AS order_line_count_global
  FROM purchase p
  JOIN cart_item co ON co.purchase_order = p.order_id
  WHERE p.merchant_id = 13927
  GROUP BY co.merchant_id
) ola ON pa.merchant_id = ola.merchant_id
LEFT JOIN (
  SELECT
    rr.merchant_id,
    SUM(rd.amount * rd.quantity) FILTER (WHERE rr.initiated_at >= (SELECT ts_min FROM period)) AS return_value_7d,
    SUM(rd.amount * rd.quantity) FILTER (WHERE rr.initiated_at >= (SELECT ts_wow FROM period)) AS return_value_wow,
    SUM(rd.amount * rd.quantity) FILTER (WHERE rr.initiated_at >= (SELECT ts_now FROM period)) AS return_value_now,
    SUM(rd.amount * rd.quantity) AS return_value_global,
    COUNT(DISTINCT rr.return_id) FILTER (WHERE rr.initiated_at >= (SELECT ts_min FROM period)) AS return_count_7d,
    COUNT(DISTINCT rr.return_id) FILTER (WHERE rr.initiated_at >= (SELECT ts_wow FROM period)) AS return_count_wow,
    COUNT(DISTINCT rr.return_id) FILTER (WHERE rr.initiated_at >= (SELECT ts_now FROM period)) AS return_count_now,
    COUNT(DISTINCT rr.return_id) AS return_count_global,
	count(*) FILTER (WHERE rr.updated_at >= (SELECT ts_min FROM period) and rr.review_status = 1) AS approve_count_7d,
	count(*) FILTER (WHERE rr.updated_at >= (SELECT ts_wow FROM period) and rr.review_status = 1) AS approve_count_wow,
	count(*) FILTER (WHERE rr.updated_at >= (SELECT ts_now FROM period) and rr.review_status = 1) AS approve_count_now,
	count(*) FILTER (where rr.review_status = 1) As approve_count_global,
	count(*) FILTER (WHERE rr.updated_at >= (SELECT ts_min FROM period) and rr.review_status = 2) AS decline_count_7d,
	count(*) FILTER (WHERE rr.updated_at >= (SELECT ts_wow FROM period) and rr.review_status = 2) AS decline_count_wow,
	count(*) FILTER (WHERE rr.updated_at >= (SELECT ts_now FROM period) and rr.review_status = 2) AS decline_count_now,
	count(*) FILTER (where rr.review_status = 2) As decline_count_global
  FROM return_request rr
  JOIN return_details rd ON rd.return_id = rr.return_id
  WHERE rr.merchant_id = 13927
  GROUP BY rr.merchant_id
) cr ON pa.merchant_id = cr.merchant_id;