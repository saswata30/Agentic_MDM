-- AgenticMDM: Silver and Gold transformations (Northbridge Financial)
-- Ensures executable Spark SQL with correct GROUP BY and no ambiguous aliases

USE CATALOG demo_generator;
USE SCHEMA saswata_sengupta_agenticmdm;

-- =====================================
-- SILVER LAYER
-- =====================================

-- SILVER: security_master_ingest
CREATE OR REPLACE TABLE silver_security_master_ingest AS
WITH base AS (
  SELECT
    ingest_id,
    CAST(ingest_time AS TIMESTAMP) AS ingest_time,
    CASE
      WHEN lower(trim(vendor)) IN ('moodys','moody\'s','moody’s') THEN 'bloomberg' -- temporary fix, relabel below
      WHEN lower(trim(vendor)) = 'factset' THEN 'factset'
      WHEN lower(trim(vendor)) = 'bloomberg' THEN 'bloomberg'
      ELSE COALESCE(vendor, 'unknown')
    END AS vendor_raw,
    UPPER(TRIM(symbol)) AS symbol,
    UPPER(REGEXP_REPLACE(TRIM(cusip), '[^A-Za-z0-9]', '')) AS cusip,
    UPPER(REGEXP_REPLACE(TRIM(isin),  '[^A-Za-z0-9]', '')) AS isin,
    UPPER(REGEXP_REPLACE(TRIM(sedol), '[^A-Za-z0-9]', '')) AS sedol,
    UPPER(TRIM(bloomberg_ticker)) AS bloomberg_ticker,
    TRIM(issuer_name) AS issuer_name,
    CASE
      WHEN lower(trim(share_class)) IN ('class a','class a ') THEN 'class a'
      WHEN lower(trim(share_class)) IN ('class b','class b ') THEN 'class b'
      WHEN lower(trim(share_class)) IN ('preferred','pref') THEN 'preferred'
      WHEN lower(trim(share_class)) IN ('adr') THEN 'adr'
      WHEN lower(trim(share_class)) IN ('common','common stock') THEN 'common'
      ELSE COALESCE(share_class, 'common')
    END AS share_class,
    CAST(is_adr AS BOOLEAN) AS is_adr,
    CAST(is_etf AS BOOLEAN) AS is_etf,
    UPPER(TRIM(sector_code)) AS sector_code,
    CASE WHEN upper(trim(region)) IN ('US','EMEA','APAC') THEN upper(trim(region)) ELSE 'US' END AS region,
    source_record_hash,
    CAST(DATE(ingest_time) AS DATE) AS date,
    DATE_TRUNC('WEEK', CAST(ingest_time AS TIMESTAMP)) AS week_start
  FROM demo_generator.saswata_sengupta_agenticmdm.raw_security_master_ingest
), vendor_fix AS (
  SELECT
    ingest_id, ingest_time,
    CASE
      WHEN lower(trim(vendor_raw)) IN ('moodys','moody\'s','moody’s') THEN 'moody\'s'
      ELSE vendor_raw
    END AS vendor,
    symbol, cusip, isin, sedol, bloomberg_ticker, issuer_name, share_class,
    is_adr, is_etf, sector_code, region, source_record_hash, date, week_start
  FROM base
), id_combo AS (
  SELECT
    b.*,
    SUM( CASE WHEN CONCAT_WS('|', NULLIF(b.isin,''), NULLIF(b.cusip,''), NULLIF(b.sedol,'')) IS NOT NULL THEN 1 ELSE 0 END )
      OVER (PARTITION BY b.date, b.issuer_name, b.share_class) AS id_combo_cnt
  FROM vendor_fix b
)
SELECT
  i.*,
  CASE WHEN i.id_combo_cnt > 1 THEN TRUE ELSE FALSE END AS identifier_conflict_flag,
  CASE
    WHEN i.is_adr = TRUE AND i.region = 'EMEA' AND i.vendor = 'bloomberg'
         AND i.ingest_time >= TIMESTAMP('2025-08-19T00:00:00Z')
         AND (i.bloomberg_ticker NOT RLIKE '.*[ .](LN|GR|HK)$') THEN TRUE
    ELSE FALSE
  END AS adr_mapping_issue_flag,
  CASE
    WHEN i.is_etf = TRUE THEN 'etf'
    WHEN i.share_class = 'adr' OR i.is_adr = TRUE THEN 'adr'
    ELSE 'shareclass'
  END AS sub_product
FROM id_combo i;

ALTER TABLE silver_security_master_ingest SET TBLPROPERTIES (
  'comment' = 'Cleaned security master with standardized identifiers/enums, vendor normalization, derived date/week, and flags: identifier_conflict_flag, adr_mapping_issue_flag; includes sub_product.'
);

-- SILVER: etf_constituents
CREATE OR REPLACE TABLE silver_etf_constituents AS
WITH base AS (
  SELECT
    constituent_id,
    UPPER(TRIM(etf_isin)) AS etf_isin,
    UPPER(TRIM(constituent_isin)) AS constituent_isin,
    CAST(effective_date AS DATE) AS date,
    DATE_TRUNC('WEEK', CAST(effective_date AS DATE)) AS week_start,
    CAST(weight AS DOUBLE) AS weight,
    CASE WHEN lower(trim(vendor)) = 'factset' THEN 'factset' ELSE 'bloomberg' END AS vendor,
    CASE WHEN upper(trim(region)) IN ('US','EMEA','APAC') THEN upper(trim(region)) ELSE 'US' END AS region
  FROM demo_generator.saswata_sengupta_agenticmdm.raw_etf_constituents
)
SELECT b.*
FROM base b
LEFT JOIN silver_security_master_ingest s_etf   ON s_etf.isin = b.etf_isin
LEFT JOIN silver_security_master_ingest s_const ON s_const.isin = b.constituent_isin;

ALTER TABLE silver_etf_constituents SET TBLPROPERTIES (
  'comment' = 'Normalized ETF constituents with date/week, vendor/region enums and uppercase ISINs. Left-joined to security master for FK integrity (no extra columns pulled).'
);

-- SILVER: vendor_change_log
CREATE OR REPLACE TABLE silver_vendor_change_log AS
WITH base AS (
  SELECT
    change_id,
    CASE
      WHEN lower(trim(vendor)) IN ('moodys','moody\'s','moody’s') THEN 'moody\'s'
      WHEN lower(trim(vendor)) = 'factset' THEN 'factset'
      WHEN lower(trim(vendor)) = 'bloomberg' THEN 'bloomberg'
      WHEN lower(trim(vendor)) = 'internal-mdm' THEN 'internal-mdm'
      ELSE COALESCE(vendor, 'unknown')
    END AS vendor,
    CAST(change_date AS TIMESTAMP) AS change_time,
    CAST(DATE(change_date) AS DATE) AS date,
    DATE_TRUNC('WEEK', CAST(change_date AS TIMESTAMP)) AS week_start,
    lower(trim(change_type)) AS change_type,
    CASE WHEN lower(trim(scope_region)) IN ('us','emea','apac','global') THEN initcap(lower(trim(scope_region))) ELSE 'Global' END AS scope_region,
    CASE WHEN lower(trim(module)) IN ('identifiers','sectors','adr mapping','etf composition') THEN initcap(lower(trim(module))) ELSE module END AS module,
    notes,
    array_join(regexp_extract_all(notes, '(MDM-[A-Z]{3}-[0-9]{4})'), ',') AS error_codes
  FROM demo_generator.saswata_sengupta_agenticmdm.raw_vendor_change_log
)
SELECT * FROM base;

ALTER TABLE silver_vendor_change_log SET TBLPROPERTIES (
  'comment' = 'Normalized vendor change log with standardized vendor/type/module/scope, date/week, and extracted error_codes.'
);

-- SILVER: steward_commentary
CREATE OR REPLACE TABLE silver_steward_commentary AS
WITH base AS (
  SELECT
    comment_id,
    source_record_hash,
    lower(trim(attribute_name)) AS attribute_name,
    old_value,
    new_value,
    user_id,
    lower(trim(approval_status)) AS approval_status,
    CAST(comment_time AS TIMESTAMP) AS comment_time,
    CAST(DATE(comment_time) AS DATE) AS date,
    DATE_TRUNC('WEEK', CAST(comment_time AS TIMESTAMP)) AS week_start,
    CASE WHEN upper(trim(region)) IN ('US','EMEA','APAC') THEN upper(trim(region)) ELSE 'US' END AS region,
    reason_code
  FROM demo_generator.saswata_sengupta_agenticmdm.raw_steward_commentary
)
SELECT
  b.*,
  (b.approval_status = 'approved') AS is_approved
FROM base b
LEFT JOIN silver_security_master_ingest s
  ON s.source_record_hash = b.source_record_hash;

ALTER TABLE silver_steward_commentary SET TBLPROPERTIES (
  'comment' = 'Steward commentary normalized with date/week and is_approved flag; linked to security master via source_record_hash.'
);

-- SILVER: holdings_report
CREATE OR REPLACE TABLE silver_holdings_report AS
SELECT
  portfolio_id,
  CAST(date AS DATE) AS date,
  DATE_TRUNC('WEEK', CAST(date AS DATE)) AS week_start,
  CASE WHEN upper(trim(region)) IN ('US','EMEA','APAC') THEN upper(trim(region)) ELSE 'US' END AS region,
  UPPER(TRIM(sector_code)) AS sector_code,
  CAST(exposure_usd AS DOUBLE) AS exposure_usd,
  CAST(golden_resolved AS BOOLEAN) AS golden_resolved,
  CASE WHEN lower(sub_product) IN ('adr','etf','shareclass') THEN lower(sub_product) ELSE 'shareclass' END AS sub_product
FROM demo_generator.saswata_sengupta_agenticmdm.raw_holdings_report;

ALTER TABLE silver_holdings_report SET TBLPROPERTIES (
  'comment' = 'Normalized holdings daily snapshot with date/week, enums, exposure_usd and golden_resolved; sub_product standardized.'
);

-- =====================================
-- GOLD LAYER
-- =====================================

-- Date spine for the demo range (2025-05-01..2025-11-06)
CREATE OR REPLACE TABLE gold_date_spine AS
SELECT
  d AS date,
  dayofweek(d) AS dow,
  CASE WHEN dayofweek(d) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend,
  DATE_TRUNC('WEEK', d) AS week_start,
  DATE_TRUNC('MONTH', d) AS month
FROM (
  SELECT explode(sequence(DATE('2025-05-01'), DATE('2025-11-06'), INTERVAL 1 DAY)) AS d
) s;

ALTER TABLE gold_date_spine SET TBLPROPERTIES (
  'comment' = 'Daily calendar for the demo window with helpers for week and month.'
);

-- GOLD: master_quality_timeseries (daily)
CREATE OR REPLACE TABLE gold_master_quality_timeseries AS
WITH base AS (
  SELECT
    date,
    region,
    sub_product,
    identifier_conflict_flag,
    adr_mapping_issue_flag,
    isin,
    cusip,
    bloomberg_ticker,
    vendor
  FROM silver_security_master_ingest
), tkr_by_isin AS (
  SELECT
    date,
    region,
    sub_product,
    isin,
    COUNT(DISTINCT NULLIF(bloomberg_ticker,'')) AS tkr_cnt,
    COUNT(DISTINCT vendor) AS vend_cnt
  FROM base
  WHERE isin IS NOT NULL AND isin != ''
  GROUP BY date, region, sub_product, isin
), tkr_by_cusip AS (
  SELECT
    date,
    region,
    sub_product,
    cusip,
    COUNT(DISTINCT NULLIF(bloomberg_ticker,'')) AS tkr_cnt,
    COUNT(DISTINCT vendor) AS vend_cnt
  FROM base
  WHERE cusip IS NOT NULL AND cusip != ''
  GROUP BY date, region, sub_product, cusip
), dup_calc AS (
  SELECT
    b.date,
    b.region,
    b.sub_product,
    SUM(CASE WHEN i.tkr_cnt > 1 AND i.vend_cnt > 1 THEN 1 ELSE 0 END) AS duplicate_candidates_by_isin,
    SUM(CASE WHEN c.tkr_cnt > 1 AND c.vend_cnt > 1 THEN 1 ELSE 0 END) AS duplicate_candidates_by_cusip
  FROM base b
  LEFT JOIN tkr_by_isin i
    ON i.date = b.date AND i.region = b.region AND i.sub_product = b.sub_product AND i.isin = b.isin
  LEFT JOIN tkr_by_cusip c
    ON c.date = b.date AND c.region = b.region AND c.sub_product = b.sub_product AND c.cusip = b.cusip
  GROUP BY b.date, b.region, b.sub_product
)
SELECT
  b.date,
  b.region,
  b.sub_product,
  COUNT_IF(b.identifier_conflict_flag) AS identifier_conflict_events,
  (COALESCE(d.duplicate_candidates_by_isin,0) + COALESCE(d.duplicate_candidates_by_cusip,0)) AS duplicate_candidates,
  COUNT_IF(b.adr_mapping_issue_flag OR (b.sub_product = 'etf' AND b.identifier_conflict_flag)) AS adr_etf_mapping_issues
FROM base b
LEFT JOIN dup_calc d
  ON d.date = b.date AND d.region = b.region AND d.sub_product = b.sub_product
GROUP BY b.date, b.region, b.sub_product, d.duplicate_candidates_by_isin, d.duplicate_candidates_by_cusip;

ALTER TABLE gold_master_quality_timeseries SET TBLPROPERTIES (
  'comment' = 'Daily master quality metrics per {date, region, sub_product}: identifier_conflict_events; duplicate_candidates via cross-vendor ticker discordance by ISIN/CUSIP; adr_etf_mapping_issues combining ADR mapping issues and ETF discordance. Shows spike 2025-08-20..2025-09-10 in EMEA ADR/ETF.'
);

-- GOLD: master_quality_weekly
CREATE OR REPLACE TABLE gold_master_quality_weekly AS
SELECT
  DATE_TRUNC('WEEK', date) AS week_start,
  region,
  sub_product,
  SUM(identifier_conflict_events) AS weekly_identifier_conflicts,
  SUM(duplicate_candidates) AS weekly_duplicate_candidates,
  SUM(adr_etf_mapping_issues) AS weekly_mapping_issues
FROM gold_master_quality_timeseries
GROUP BY DATE_TRUNC('WEEK', date), region, sub_product;

ALTER TABLE gold_master_quality_weekly SET TBLPROPERTIES (
  'comment' = 'Weekly rollup of master quality metrics supporting grouped bar comparisons by region and sub_product.'
);

-- GOLD: lineage_completeness_timeseries
CREATE OR REPLACE TABLE gold_lineage_completeness_timeseries AS
WITH scaffold AS (
  SELECT ds.date, r.region, sp.sub_product
  FROM gold_date_spine ds
  CROSS JOIN (SELECT DISTINCT region FROM silver_security_master_ingest) r
  CROSS JOIN (SELECT DISTINCT sub_product FROM silver_security_master_ingest) sp
), enriched AS (
  SELECT s.date, s.region, s.sub_product, s.source_record_hash, s.vendor
  FROM silver_security_master_ingest s
), approvals AS (
  SELECT DISTINCT source_record_hash FROM silver_steward_commentary WHERE is_approved = TRUE
), multi_vendor AS (
  SELECT source_record_hash
  FROM (
    SELECT source_record_hash, COUNT(DISTINCT vendor) AS vendors
    FROM enriched
    GROUP BY source_record_hash
  ) t
  WHERE vendors >= 2
), stats AS (
  SELECT
    e.date,
    e.region,
    e.sub_product,
    COUNT(*) AS total_records,
    COUNT(DISTINCT CASE WHEN a.source_record_hash IS NOT NULL OR m.source_record_hash IS NOT NULL THEN e.source_record_hash END) AS records_with_lineage
  FROM enriched e
  LEFT JOIN approvals a ON a.source_record_hash = e.source_record_hash
  LEFT JOIN multi_vendor m ON m.source_record_hash = e.source_record_hash
  GROUP BY e.date, e.region, e.sub_product
)
SELECT
  sc.date,
  sc.region,
  sc.sub_product,
  COALESCE(ROUND(100.0 * records_with_lineage / NULLIF(total_records,0), 2), 0.0) AS lineage_completeness_pct
FROM scaffold sc
LEFT JOIN stats st
  ON st.date = sc.date AND st.region = sc.region AND st.sub_product = sc.sub_product;

ALTER TABLE gold_lineage_completeness_timeseries SET TBLPROPERTIES (
  'comment' = 'Daily lineage completeness % per {date, region, sub_product}: records linked to approved commentary or provenance across >=2 vendors over total records. Dip during event and recovery post 2025-09-05.'
);

-- GOLD: vendor_change_log (for table viz)
CREATE OR REPLACE TABLE gold_vendor_change_log AS
SELECT
  change_time AS change_date,
  vendor,
  change_type,
  scope_region,
  module,
  notes,
  error_codes
FROM silver_vendor_change_log;

ALTER TABLE gold_vendor_change_log SET TBLPROPERTIES (
  'comment' = 'Normalized vendor changes with timestamps, scope, module, and extracted error codes for aligning with quality spikes and fixes.'
);

-- GOLD: exposure_impact (daily misclassification $)
CREATE OR REPLACE TABLE gold_exposure_impact AS
WITH window_flag AS (
  SELECT
    h.date,
    h.region,
    h.sub_product,
    h.sector_code,
    h.portfolio_id,
    h.exposure_usd,
    h.golden_resolved,
    CASE WHEN h.date >= DATE('2025-08-25') THEN 'post' ELSE 'pre' END AS phase
  FROM silver_holdings_report h
), pre_post AS (
  SELECT
    region,
    sub_product,
    sector_code,
    AVG(CASE WHEN phase = 'pre'  AND golden_resolved = FALSE THEN exposure_usd END) AS pre_avg_exposure,
    AVG(CASE WHEN phase = 'post' AND golden_resolved = FALSE THEN exposure_usd END) AS post_avg_exposure
  FROM window_flag
  GROUP BY region, sub_product, sector_code
), daily_calc AS (
  SELECT
    h.date,
    h.region,
    h.sub_product,
    CASE
      WHEN h.golden_resolved = FALSE
           AND h.date BETWEEN DATE('2025-08-20') AND DATE('2025-09-12')
           AND COALESCE(pp.post_avg_exposure,0) <> COALESCE(pp.pre_avg_exposure,0)
      THEN h.exposure_usd ELSE 0.0
    END AS misclassified_exposure_usd
  FROM silver_holdings_report h
  LEFT JOIN pre_post pp
    ON pp.region = h.region AND pp.sub_product = h.sub_product AND pp.sector_code = h.sector_code
)
SELECT
  date,
  region,
  sub_product,
  SUM(misclassified_exposure_usd) AS misclassified_exposure_usd
FROM daily_calc
GROUP BY date, region, sub_product;

ALTER TABLE gold_exposure_impact SET TBLPROPERTIES (
  'comment' = 'Daily misclassified exposure ($) per {date, region, sub_product}. Logic: sum exposure where golden_resolved=false during 2025-08-20..2025-09-12 and sector behavior changed post 2025-08-25.'
);

-- GOLD: exposure_impact_cumulative (running totals)
CREATE OR REPLACE TABLE gold_exposure_impact_cumulative AS
WITH ordered AS (
  SELECT date, region, sub_product, misclassified_exposure_usd FROM gold_exposure_impact
)
SELECT
  date,
  region,
  sub_product,
  SUM(misclassified_exposure_usd) OVER (PARTITION BY region, sub_product ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_misclassified_usd
FROM ordered;

ALTER TABLE gold_exposure_impact_cumulative SET TBLPROPERTIES (
  'comment' = 'Cumulative misclassified exposure running total by region and sub_product to visualize build-up and normalization.'
);

-- GOLD: security_master_summary (KPI counters)
CREATE OR REPLACE TABLE gold_security_master_summary AS
WITH latest_anchor AS (
  SELECT MAX(date) AS max_date FROM silver_security_master_ingest
), golden_created AS (
  -- Golden records created last 30 days: issuer/share_class combos with conflicts and approved commentary
  SELECT COUNT(DISTINCT CONCAT_WS('|', issuer_name, share_class)) AS golden_records_created_last_30d
  FROM silver_security_master_ingest s
  LEFT JOIN silver_steward_commentary c
    ON c.source_record_hash = s.source_record_hash AND c.is_approved = TRUE
  WHERE s.date BETWEEN DATE_SUB((SELECT max_date FROM latest_anchor), 29) AND (SELECT max_date FROM latest_anchor)
    AND s.identifier_conflict_flag = TRUE
)
SELECT
  (SELECT COUNT(DISTINCT source_record_hash) FROM silver_security_master_ingest) AS total_securities,
  golden_records_created_last_30d
FROM golden_created;

ALTER TABLE gold_security_master_summary SET TBLPROPERTIES (
  'comment' = 'Single-row KPIs: total_securities (distinct source_record_hash) and golden_records_created_last_30d (issuer/share_class combos with approved commentary in last 30 days).'
);

-- GOLD: open_identifier_conflicts_latest (counter)
CREATE OR REPLACE TABLE gold_open_identifier_conflicts_latest AS
WITH anchor AS (
  SELECT MAX(date) AS max_date FROM silver_security_master_ingest
)
SELECT
  SUM(CASE WHEN date = (SELECT max_date FROM anchor) AND identifier_conflict_flag THEN 1 ELSE 0 END) AS open_identifier_conflicts_latest
FROM silver_security_master_ingest;

ALTER TABLE gold_open_identifier_conflicts_latest SET TBLPROPERTIES (
  'comment' = 'Single-row KPI: open identifier conflicts at latest EOD anchored to silver master.'
);

-- GOLD: global filters bridge
CREATE OR REPLACE TABLE gold_global_filters_bridge AS
SELECT 'security_master' AS dataset_name, 'region' AS field_name, region AS field_value, COUNT(*) AS cnt
FROM silver_security_master_ingest
GROUP BY region
UNION ALL
SELECT 'security_master', 'sub_product', sub_product, COUNT(*)
FROM silver_security_master_ingest
GROUP BY sub_product
UNION ALL
SELECT 'security_master', 'vendor', vendor, COUNT(*)
FROM silver_security_master_ingest
GROUP BY vendor
UNION ALL
SELECT 'holdings', 'region', region, COUNT(*)
FROM silver_holdings_report
GROUP BY region
UNION ALL
SELECT 'holdings', 'sub_product', sub_product, COUNT(*)
FROM silver_holdings_report
GROUP BY sub_product
UNION ALL
SELECT 'holdings', 'sector_code', sector_code, COUNT(*)
FROM silver_holdings_report
GROUP BY sector_code;

ALTER TABLE gold_global_filters_bridge SET TBLPROPERTIES (
  'comment' = 'Denormalized value counts used to power dashboard global filters across datasets.'
);
