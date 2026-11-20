-- OntoBricks Evolve evaluation fixture: a small supply-chain dataset.
--
-- Placeholders: __CATALOG__ and __SCHEMA__ are replaced by the seeder.
-- Every table is created with CREATE OR REPLACE so a reseed is reproducible.
-- The tables are written once per seed and never updated, so their Delta
-- versions are stable for the duration of an evaluation run.
--
-- Expected answers (authored by hand, re-checked by gold_check.sql):
--   Orders that contain a product supplied by Acme  -> O1, O3, O5
--   Products supplied by Acme                       -> P1, P3
--   Order lines of order O1                         -> L1, L2
--   (full list in gold_answers below)

CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.suppliers (
  supplier_id STRING NOT NULL,
  supplier_name STRING,
  CONSTRAINT suppliers_pk PRIMARY KEY (supplier_id)
);
INSERT INTO __CATALOG__.__SCHEMA__.suppliers VALUES
  ('S1', 'Acme'), ('S2', 'Globex'), ('S3', 'Initech'), ('S4', 'Umbrella');

CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.products (
  product_id STRING NOT NULL,
  product_name STRING,
  sku STRING,
  supplier_id STRING,
  CONSTRAINT products_pk PRIMARY KEY (product_id),
  CONSTRAINT products_supplier_fk FOREIGN KEY (supplier_id)
    REFERENCES __CATALOG__.__SCHEMA__.suppliers (supplier_id)
);
INSERT INTO __CATALOG__.__SCHEMA__.products VALUES
  ('P1', 'Widget', 'W-001', 'S1'),
  ('P2', 'Gadget', 'G-002', 'S2'),
  ('P3', 'Sprocket', 'S-003', 'S1'),
  ('P4', 'Gizmo', 'Z-004', 'S3'),
  ('P5', 'Doohickey', 'D-005', 'S4');

CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.orders (
  order_id STRING NOT NULL,
  customer_name STRING,
  order_date DATE,
  CONSTRAINT orders_pk PRIMARY KEY (order_id)
);
INSERT INTO __CATALOG__.__SCHEMA__.orders VALUES
  ('O1', 'Northwind', DATE'2026-01-05'),
  ('O2', 'Contoso', DATE'2026-01-09'),
  ('O3', 'Fabrikam', DATE'2026-02-01'),
  ('O4', 'Northwind', DATE'2026-02-14'),
  ('O5', 'Tailspin', DATE'2026-03-02'),
  ('O6', 'Contoso', DATE'2026-03-20');

-- order_lines.product_id has a declared FK to products. alt_product_ref holds
-- the product SKU, a second column that also joins to products (on sku).
CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.order_lines (
  line_id STRING NOT NULL,
  order_id STRING,
  product_id STRING,
  alt_product_ref STRING,
  quantity INT,
  CONSTRAINT order_lines_pk PRIMARY KEY (line_id),
  CONSTRAINT order_lines_order_fk FOREIGN KEY (order_id)
    REFERENCES __CATALOG__.__SCHEMA__.orders (order_id),
  CONSTRAINT order_lines_product_fk FOREIGN KEY (product_id)
    REFERENCES __CATALOG__.__SCHEMA__.products (product_id)
);
INSERT INTO __CATALOG__.__SCHEMA__.order_lines VALUES
  ('L1', 'O1', 'P1', 'W-001', 2),
  ('L2', 'O1', 'P2', 'G-002', 1),
  ('L3', 'O2', 'P2', 'G-002', 5),
  ('L4', 'O3', 'P3', 'S-003', 3),
  ('L5', 'O4', 'P4', 'Z-004', 1),
  ('L6', 'O5', 'P1', 'W-001', 4),
  ('L7', 'O5', 'P5', 'D-005', 2),
  ('L8', 'O6', 'P5', 'D-005', 6);

-- Variant: same order lines with no declared keys, so the two joins to
-- products (product_id and alt_product_ref->sku) are equally plausible.
CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.order_lines_nofk AS
  SELECT * FROM __CATALOG__.__SCHEMA__.order_lines;

-- Variant: an order line pointing at a product that does not exist.
CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.order_lines_orphan AS
  SELECT * FROM __CATALOG__.__SCHEMA__.order_lines
  UNION ALL SELECT 'L9', 'O6', 'P99', 'X-999', 1;

-- Variant: product P1 appears twice with different suppliers.
CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.products_dup AS
  SELECT * FROM __CATALOG__.__SCHEMA__.products
  UNION ALL SELECT 'P1', 'Widget (relabelled)', 'W-001', 'S2';

-- Variant: two suppliers share the name Acme.
CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.suppliers_ambiguous AS
  SELECT * FROM __CATALOG__.__SCHEMA__.suppliers
  UNION ALL SELECT 'S5', 'Acme';

-- A table the restricted test principal must not read.
CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.restricted_costs (
  product_id STRING NOT NULL,
  unit_cost DECIMAL(10, 2)
);
INSERT INTO __CATALOG__.__SCHEMA__.restricted_costs VALUES
  ('P1', 3.10), ('P2', 7.25), ('P3', 1.40), ('P4', 9.99), ('P5', 0.55);

-- Gold answers, authored by hand. gold_check.sql recomputes them from the
-- source tables with plain SQL that does not use the ontology.
CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.gold_answers (
  question_id STRING NOT NULL,
  expected_ids ARRAY<STRING>
);
INSERT INTO __CATALOG__.__SCHEMA__.gold_answers VALUES
  ('acme_orders', ARRAY('O1', 'O3', 'O5')),
  ('globex_orders', ARRAY('O1', 'O2')),
  ('umbrella_orders', ARRAY('O5', 'O6')),
  ('initech_orders', ARRAY('O4')),
  ('acme_products', ARRAY('P1', 'P3')),
  ('globex_products', ARRAY('P2')),
  ('o1_lines', ARRAY('L1', 'L2')),
  ('o5_lines', ARRAY('L6', 'L7')),
  ('northwind_lines', ARRAY('L1', 'L2', 'L5'));
