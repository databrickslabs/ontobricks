-- Recompute every gold answer from the source tables with plain SQL.
-- Returns one row per question with a boolean `matches`; all must be true.
WITH computed AS (
  SELECT 'acme_orders' AS question_id,
         array_sort(collect_set(ol.order_id)) AS ids
  FROM __CATALOG__.__SCHEMA__.order_lines ol
  JOIN __CATALOG__.__SCHEMA__.products p ON ol.product_id = p.product_id
  JOIN __CATALOG__.__SCHEMA__.suppliers s ON p.supplier_id = s.supplier_id
  WHERE s.supplier_name = 'Acme'
  UNION ALL
  SELECT 'globex_orders', array_sort(collect_set(ol.order_id))
  FROM __CATALOG__.__SCHEMA__.order_lines ol
  JOIN __CATALOG__.__SCHEMA__.products p ON ol.product_id = p.product_id
  JOIN __CATALOG__.__SCHEMA__.suppliers s ON p.supplier_id = s.supplier_id
  WHERE s.supplier_name = 'Globex'
  UNION ALL
  SELECT 'umbrella_orders', array_sort(collect_set(ol.order_id))
  FROM __CATALOG__.__SCHEMA__.order_lines ol
  JOIN __CATALOG__.__SCHEMA__.products p ON ol.product_id = p.product_id
  JOIN __CATALOG__.__SCHEMA__.suppliers s ON p.supplier_id = s.supplier_id
  WHERE s.supplier_name = 'Umbrella'
  UNION ALL
  SELECT 'acme_products', array_sort(collect_set(p.product_id))
  FROM __CATALOG__.__SCHEMA__.products p
  JOIN __CATALOG__.__SCHEMA__.suppliers s ON p.supplier_id = s.supplier_id
  WHERE s.supplier_name = 'Acme'
  UNION ALL
  SELECT 'o1_lines', array_sort(collect_set(line_id))
  FROM __CATALOG__.__SCHEMA__.order_lines
  WHERE order_id = 'O1'
  UNION ALL
  SELECT 'initech_orders', array_sort(collect_set(ol.order_id))
  FROM __CATALOG__.__SCHEMA__.order_lines ol
  JOIN __CATALOG__.__SCHEMA__.products p ON ol.product_id = p.product_id
  JOIN __CATALOG__.__SCHEMA__.suppliers s ON p.supplier_id = s.supplier_id
  WHERE s.supplier_name = 'Initech'
  UNION ALL
  SELECT 'globex_products', array_sort(collect_set(p.product_id))
  FROM __CATALOG__.__SCHEMA__.products p
  JOIN __CATALOG__.__SCHEMA__.suppliers s ON p.supplier_id = s.supplier_id
  WHERE s.supplier_name = 'Globex'
  UNION ALL
  SELECT 'o5_lines', array_sort(collect_set(line_id))
  FROM __CATALOG__.__SCHEMA__.order_lines
  WHERE order_id = 'O5'
  UNION ALL
  SELECT 'northwind_lines', array_sort(collect_set(ol.line_id))
  FROM __CATALOG__.__SCHEMA__.order_lines ol
  JOIN __CATALOG__.__SCHEMA__.orders o ON ol.order_id = o.order_id
  WHERE o.customer_name = 'Northwind'
)
SELECT g.question_id,
       array_sort(g.expected_ids) AS expected,
       c.ids AS computed,
       array_sort(g.expected_ids) = c.ids AS matches
FROM __CATALOG__.__SCHEMA__.gold_answers g
LEFT JOIN computed c ON g.question_id = c.question_id
ORDER BY g.question_id;
