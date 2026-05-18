-- Databricks notebook source
-- MAGIC %md
-- MAGIC # process_CABS_Usage_Summary
-- MAGIC   This notebook processes telecom usage data by aggregating, transforming, and summarizing call records from multiple staging tables (SS7, AMA, EMI). It identifies wireless companies, updates relevant flags, and computes usage totals, including special handling for CenturyLink CICs. The workflow loads summarized data into reporting tables, applies data quality filters, and pivots results for daily reporting. Additional logic groups usage by company and IXC, and prepares AMA-specific summaries for further analysis.
-- MAGIC
-- MAGIC   output tables :
-- MAGIC  1. exc_CABS_no_AMA_Usage_Detail
-- MAGIC  2. usg_rt_CABS_Usage_Summary
-- MAGIC  3. usg_CABS_SS7_MOU_EMI_Daily_Totals
-- MAGIC  4. usg_rt_CABS_CO_NBR_IXC_Usage_Summary
-- MAGIC  5. usg_AMA_for_Load_Summary
-- MAGIC   

-- COMMAND ----------

-- Notebook Header
-- Title: usg_sp_CABS_Usage_Summary
-- Description: This notebook processes telecom usage data from various sources through a series of SQL transformations.
-- Author: 
-- Created Date: 26/9/2025    Last Modified: 10/10/2025
-- Version: 
-- Change Log:
--   - 10/10/2025: optimization
--   - [Date]: [Description of changes made]


-- COMMAND ----------

use dev_udl_dlh_finance.ra_voice;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_tmp_wireless_companies AS
SELECT DISTINCT SW_Cell_Nbr
FROM ref_centurylink_company_codes
WHERE SW_Cell_Nbr IS NOT NULL;



-- COMMAND ----------

MERGE INTO usg_ama_orig AS target
USING (SELECT SW_Cell_Nbr FROM v_tmp_wireless_companies) AS source
ON CAST(target.COMPANY_NUMBER AS DOUBLE) = source.SW_Cell_Nbr
WHEN MATCHED THEN UPDATE SET target.sw_cell_co = 1;

MERGE INTO usg_ama_term AS target
USING (SELECT SW_Cell_Nbr FROM v_tmp_wireless_companies) AS source
ON CAST(target.COMPANY_NUMBER AS DOUBLE) = source.SW_Cell_Nbr
WHEN MATCHED THEN UPDATE SET target.sw_cell_co = 1;

MERGE INTO usg_emi_orig AS target
USING (SELECT SW_Cell_Nbr FROM v_tmp_wireless_companies) AS source
ON CAST(target.COMPANY_NUMBER AS DOUBLE) = source.SW_Cell_Nbr
WHEN MATCHED THEN UPDATE SET target.sw_cell_co = 1;

MERGE INTO usg_emi_term AS target
USING (SELECT SW_Cell_Nbr FROM v_tmp_wireless_companies) AS source
ON CAST(target.COMPANY_NUMBER AS DOUBLE) = source.SW_Cell_Nbr
WHEN MATCHED THEN UPDATE SET target.sw_cell_co = 1;

-- COMMAND ----------

-- Optional: SET run_id = 12345; -- no longer used

CREATE OR REPLACE TABLE tmp_cabs_totals AS
SELECT
  source,
  dateforstartofcharge,
  direction,
  item,
  rate_code,
  rate_class,
  cdrs,
  mou,
  CALL_HOUR,
  job_id
FROM (
  SELECT
    'SS7' AS source,
    dateforstartofcharge,
    direction,
    item,
    rate_code,
    CAST(NULL AS STRING) AS rate_class,
    SUM(cdrs) AS cdrs,
    SUM(mou) AS mou,
    CALL_HOUR,
    job_id
  FROM usg_SS7_Orig
  GROUP BY dateforstartofcharge, direction, item, rate_code, CALL_HOUR, job_id

  UNION ALL

  SELECT
    'SS7' AS source,
    dateforstartofcharge,
    direction,
    item,
    rate_code,
    CAST(NULL AS STRING) AS rate_class,
    SUM(cdrs) AS cdrs,
    SUM(mou) AS mou,
    CALL_HOUR,
    job_id
  FROM usg_SS7_Term
  GROUP BY dateforstartofcharge, direction, item, rate_code, CALL_HOUR, job_id

  UNION ALL

  SELECT
    'AMA' AS source,
    dateforstartofcharge,
    direction,
    item,
    CAST(NULL AS STRING) AS rate_code,
    rate_class,
    SUM(cdrs) AS cdrs,
    SUM(mou) AS mou,
    CALL_HOUR,
    job_id
  FROM usg_AMA_Orig
  GROUP BY dateforstartofcharge, direction, item, rate_class, CALL_HOUR, job_id

  UNION ALL

  SELECT
    'AMA' AS source,
    dateforstartofcharge,
    direction,
    item,
    CAST(NULL AS STRING) AS rate_code,
    rate_class,
    SUM(cdrs) AS cdrs,
    SUM(mou) AS mou,
    CALL_HOUR,
    job_id
  FROM usg_AMA_Term
  GROUP BY dateforstartofcharge, direction, item, rate_class, CALL_HOUR, job_id

  UNION ALL

  SELECT
    'EMI' AS source,
    dateforstartofcharge,
    direction,
    item,
    CAST(NULL AS STRING) AS rate_code,
    rate_class,
    SUM(cdrs) AS cdrs,
    SUM(mou) AS mou,
    CALL_HOUR,
    job_id
  FROM usg_emi_orig
  GROUP BY dateforstartofcharge, direction, item, rate_class, CALL_HOUR, job_id

  UNION ALL

  SELECT
    'EMI' AS source,
    dateforstartofcharge,
    direction,
    item,
    CAST(NULL AS STRING) AS rate_code,
    rate_class,
    SUM(cdrs) AS cdrs,
    SUM(mou) AS mou,
    CALL_HOUR,
    job_id
  FROM usg_emi_term
  GROUP BY dateforstartofcharge, direction, item, rate_class, CALL_HOUR, job_id
) sub;

-- COMMAND ----------

CREATE or REPLACE TABLE usg_rt_CABS_Usage_Summary(
	source string NOT NULL,
	dateforstartofcharge timestamp ,
	direction string ,
	item string ,
	rate_code string,
	rate_class string,
	cdrs decimal(38,2) ,
	mou decimal(38,2) ,
	CALL_HOUR int  ,
	job_id bigint ,
	cdrs_CTL_CIC decimal(38,2) , 
	mou_CTL_CIC decimal(38,2)  
) using delta ;

-- COMMAND ----------

DELETE FROM usg_rt_CABS_Usage_Summary
WHERE EXISTS (
  SELECT 1
  FROM tmp_CABS_Totals t
  WHERE usg_rt_CABS_Usage_Summary.dateforstartofcharge = t.dateforstartofcharge
    AND usg_rt_CABS_Usage_Summary.source = t.source
    AND usg_rt_CABS_Usage_Summary.direction = t.direction
    AND usg_rt_CABS_Usage_Summary.job_id = t.job_id
);

-- COMMAND ----------

INSERT INTO usg_rt_CABS_Usage_Summary (
   source,  dateforstartofcharge,
  direction, item, rate_code, rate_class, cdrs, mou, CALL_HOUR, job_id
)
SELECT 
 source,  dateforstartofcharge,
  direction, item, rate_code, rate_class, cdrs, mou, CALL_HOUR, job_id
FROM tmp_CABS_Totals;


-- COMMAND ----------

-- Delete rows where dateforstartofcharge is in the future
-- or older than 13 months before end of last month

DELETE FROM usg_rt_CABS_Usage_Summary
WHERE dateforstartofcharge > current_timestamp()
   OR dateforstartofcharge < date_add(last_day(add_months(current_date(), -13)), 1);


-- COMMAND ----------

-- it is  more optimized due to centralized logic, reduced redundancy, and better potential for broadcast and caching.
CREATE OR REPLACE TEMP VIEW v_tmp_CTL_CIC_Totals AS
WITH ctl_cic AS (
  SELECT DISTINCT CIC FROM ref_CenturyLink_CICs
)
SELECT 'SS7' AS source, dateforstartofcharge, direction, item, rate_code,
       NULL AS rate_class, SUM(cdrs) AS cdrs_CTL_CIC, SUM(mou) AS mou_CTL_CIC, CALL_HOUR, job_id
FROM (
  SELECT dateforstartofcharge, direction, item, rate_code, CALL_HOUR, job_id, cdrs, mou, tg_cic_fgd
  FROM usg_SS7_Orig
  UNION ALL
  SELECT dateforstartofcharge, direction, item, rate_code, CALL_HOUR, job_id, cdrs, mou, tg_cic_fgd
  FROM usg_SS7_Term
) ss7
WHERE tg_cic_fgd IN (SELECT CIC FROM ctl_cic)
GROUP BY dateforstartofcharge, direction, item, rate_code, CALL_HOUR, job_id;


-- COMMAND ----------

WITH deduped_source AS (
  SELECT
    dateforstartofcharge,
    source,
    direction,
    job_id,
    cdrs_CTL_CIC,
    mou_CTL_CIC
  FROM v_tmp_CTL_CIC_Totals
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY dateforstartofcharge, source, direction, job_id
    ORDER BY dateforstartofcharge
  ) = 1
)
MERGE INTO usg_rt_CABS_Usage_Summary AS target
USING deduped_source AS source
ON target.dateforstartofcharge = source.dateforstartofcharge
   AND target.source = source.source
   AND target.direction = source.direction
   AND target.job_id = source.job_id
WHEN MATCHED THEN
  UPDATE SET
    target.cdrs_CTL_CIC = source.cdrs_CTL_CIC,
    target.mou_CTL_CIC = source.mou_CTL_CIC;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_tmp_pivot_refresh AS
SELECT
  dateforstartofcharge,
  direction,
  col,
  SUM(CASE WHEN source = 'SS7' THEN value ELSE 0 END) AS SS7,
  SUM(CASE WHEN source = 'AMA' THEN value ELSE 0 END) AS AMA,
  SUM(CASE WHEN source = 'EMI' THEN value ELSE 0 END) AS EMI
FROM (
  SELECT
    dateforstartofcharge,
    direction,
    source,
    col,
    value
  FROM usg_rt_CABS_Usage_Summary
  LATERAL VIEW inline(array(
    struct('mou' AS col, mou AS value),
    struct('cdrs' AS col, cdrs AS value),
    struct('mou_CTL_CIC' AS col, MOU_CTL_CIC AS value),
    struct('cdrs_CTL_CIC' AS col, cdrs_CTL_CIC AS value)
  )) c
) a
GROUP BY dateforstartofcharge, direction, col;

-- COMMAND ----------

CREATE or replace TABLE usg_CABS_SS7_MOU_EMI_Daily_Totals(
	
	dateforstartofcharge timestamp ,
	direction string ,
	col string ,
	SS7 decimal( 38, 2) ,
	AMA decimal( 38, 2) ,
	EMI decimal( 38, 2) 
) 


-- COMMAND ----------

INSERT INTO usg_CABS_SS7_MOU_EMI_Daily_Totals (

  dateforstartofcharge,
  direction,
  col,
  SS7,
  AMA,
  EMI
)
SELECT 
  
  dateforstartofcharge,
  direction,
  col,
  SS7,
  AMA,
  EMI
FROM v_tmp_pivot_refresh;


-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW v_tmp_CO_NBR_IXC_Totals AS
SELECT source, dateforstartofcharge, direction, COMPANY_NUMBER, ixc, SUM(cdrs) AS cdrs, SUM(mou) AS mou, job_id
FROM (
  SELECT 
    'AMA' AS source, 
    dateforstartofcharge, 
    direction, 
    COMPANY_NUMBER,
    CASE 
      WHEN sw_cell_co = 1 OR (LEFT(company_number, 1) = '7' AND ixc = '0000') THEN AMA_TERM_OCN 
      ELSE ixc 
    END AS ixc,
    cdrs, 
    mou, 
    job_id
  FROM usg_AMA_Orig

  UNION ALL

  SELECT 
    'AMA' AS source,  
    dateforstartofcharge, 
    direction, 
    COMPANY_NUMBER,
    CASE 
      WHEN sw_cell_co = 1 OR (LEFT(company_number, 1) = '7' AND ixc = '0000') THEN AMA_ORIG_OCN 
      ELSE ixc 
    END AS ixc,
    cdrs, 
    mou, 
    job_id
  FROM usg_AMA_Term

  UNION ALL

  SELECT 
    'EMI' AS source,  
    dateforstartofcharge, 
    direction, 
    COMPANY_NUMBER,
    CASE 
      WHEN sw_cell_co = 1 OR (LEFT(company_number, 1) = '7' AND ixc = '0000') THEN AMA_TERM_OCN 
      ELSE ixc 
    END AS ixc,
    cdrs, 
    mou, 
    job_id
  FROM usg_emi_orig

  UNION ALL

  SELECT 
    'EMI' AS source,  
    dateforstartofcharge, 
    direction, 
    COMPANY_NUMBER,
    CASE 
      WHEN sw_cell_co = 1 OR (LEFT(company_number, 1) = '7' AND ixc = '0000') THEN AMA_ORIG_OCN 
      ELSE ixc 
    END AS ixc,
    cdrs, 
    mou, 
    job_id
  FROM usg_emi_term
) t
GROUP BY source, dateforstartofcharge, direction, COMPANY_NUMBER, ixc, job_id;

-- COMMAND ----------


CREATE or REPLACE TABLE  usg_rt_CABS_CO_NBR_IXC_Usage_Summary(
	source string  NOT null ,
	dateforstartofcharge timestamp ,
	direction string ,
	COMPANY_NUMBER string ,
	ixc string ,
	job_id bigint ,
	cdrs decimal( 38 , 2) ,
	mou decimal( 38 , 2) 
) using delta ;


-- COMMAND ----------


DELETE FROM usg_rt_CABS_CO_NBR_IXC_Usage_Summary
WHERE EXISTS (
  SELECT 1
  FROM v_tmp_CO_NBR_IXC_Totals t
  WHERE usg_rt_CABS_CO_NBR_IXC_Usage_Summary.dateforstartofcharge = t.dateforstartofcharge
    AND usg_rt_CABS_CO_NBR_IXC_Usage_Summary.source = t.source
    AND usg_rt_CABS_CO_NBR_IXC_Usage_Summary.direction = t.direction
    AND usg_rt_CABS_CO_NBR_IXC_Usage_Summary.job_id = t.job_id
);


-- COMMAND ----------

INSERT INTO usg_rt_CABS_CO_NBR_IXC_Usage_Summary (
  
  source,
  
  dateforstartofcharge,
  direction,
  company_number,
  ixc,
  job_id,
  cdrs,
  mou
)
SELECT 
  
  source,
 
  dateforstartofcharge,
  direction,
  company_number,
  ixc,
  job_id,
  cdrs,
  mou
FROM v_tmp_CO_NBR_IXC_Totals;


-- COMMAND ----------

DELETE FROM usg_rt_CABS_CO_NBR_IXC_Usage_Summary
WHERE dateforstartofcharge > current_timestamp()
   OR dateforstartofcharge < date_add(last_day(add_months(current_date(), -13)), 1);



-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW v_AMA_for_summary AS
SELECT 

  source,
  dateforstartofcharge AS Call_date,
  dateforstartofcharge AS Process_date,
  SUM(cdrs) AS cdrs,
  SUM(mou) AS mou,
  0 AS rated_mou,
  0 AS revenue,
  job_id,
  'unknown' AS product,
  'Green' AS carrier
FROM usg_rt_CABS_Usage_Summary
WHERE source = 'AMA'
GROUP BY  source, dateforstartofcharge, job_id;


-- COMMAND ----------

CREATE or REPLACE TABLE usg_AMA_for_Load_Summary(
	source string NOT null,
	Call_date timestamp ,
	Process_date timestamp ,
	cdrs decimal(38,2) ,
	mou decimal(38,2) ,
	rated_mou int NOT null ,
	revenue int NOT null,
	job_id bigint ,
	product string NOT null,
	carrier string NOT  null
) 

-- COMMAND ----------

INSERT INTO usg_AMA_for_Load_Summary (
  
  source,
  Call_date,
  Process_date,
  cdrs,
  mou,
  rated_mou,
  revenue,
  job_id,
  product,
  carrier
)
SELECT 
 
  a.source,
  a.Call_date,
  a.Process_date,
  a.cdrs,
  a.mou,
  a.rated_mou,
  a.revenue,
  a.job_id,
  a.product,
  a.carrier
FROM v_AMA_for_summary a
LEFT JOIN usg_AMA_for_Load_Summary b
  ON a.job_id = b.job_id AND a.Call_date = b.Call_date
WHERE b.job_id IS NULL AND b.Call_date IS NULL;


-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW v_tmp_tg_compare AS
SELECT DISTINCT 
  ss7.*, 
  ama.ama_peg_total, 
  ama.ama_duration_total,
  trunk.tgn AS inv_tgn, 
  trunk.item AS inv_item, 
  trunk.CLLI_NE AS inv_clli_ne, 
  trunk.CLLI_FE AS ivc_clli_fe, 
  trunk.AMA_FLAG, 
  trunk.RATE_CODE
FROM (
  SELECT 
    ROW_NUMBER() OVER (
      ORDER BY DATE_FOR_START_OF_CHARGE, tgn, item_no, recording_clli, far_clli, direction
    ) AS row_id,
    DATE_FOR_START_OF_CHARGE AS date,
    tgn, item_no, recording_clli, far_clli,
    trunk_group_desc, traffic_desc, direction,
    SUM(ss7_peg_total) AS ss7_peg_total,
    SUM(ss7_duration_total) / 60000 AS ss7_duration_total
  FROM usg_MOU_SS7_TGN_Report
  WHERE near_tco != 'T600'
  GROUP BY DATE_FOR_START_OF_CHARGE, tgn, item_no, recording_clli, far_clli, trunk_group_desc, traffic_desc, direction
) ss7
LEFT JOIN (
  SELECT 
    DATE_FOR_START_OF_CHARGE AS date,
    tgn, item_no, recording_clli, far_clli,
    trunk_group_desc, traffic_desc, direction,
    SUM(ama_peg_total) AS ama_peg_total,
    SUM(ama_duration_total) / 60000 AS ama_duration_total
  FROM usg_MOU_AMA_TGN_Report
  GROUP BY DATE_FOR_START_OF_CHARGE, tgn, item_no, recording_clli, far_clli, trunk_group_desc, traffic_desc, direction
) ama
ON ss7.date = ama.date
   AND ss7.tgn = ama.tgn
   AND ss7.item_no = ama.item_no
   AND ss7.direction = ama.direction
   AND ss7.recording_clli = ama.recording_clli
   AND ss7.far_clli = ama.far_clli
LEFT JOIN (
  SELECT DISTINCT tgn, item, clli_ne, clli_fe, AMA_FLAG, RATE_CODE
  FROM ref_MOU_TRUNK_GROUP ---
) trunk
ON ss7.tgn = trunk.tgn
   AND CAST(ss7.item_no AS INT) = trunk.item
   AND ss7.recording_clli = trunk.clli_ne
   AND ss7.far_clli = trunk.clli_fe;


-- COMMAND ----------

CREATE or replace TABLE exc_CABS_no_AMA_Usage_Detail(
	date timestamp ,
	tgn string ,
	item_no string ,
	recording_clli string ,
	far_clli string ,
	trunk_group_desc string ,
	traffic_desc string ,
	direction string ,
	ss7_peg_total decimal( 38 , 2) ,
	ss7_duration_total decimal(38, 6) ,
	ama_peg_total decimal( 38 , 2) ,
	ama_duration_total decimal(38, 2) ,
	inv_tgn string ,
	inv_item bigint  ,
	inv_clli_ne string,
	ivc_clli_fe string,
	AMA_FLAG string ,
	RATE_CODE string ,
	exception_row bigint ,
	row_id BIGINT GENERATED ALWAYS AS IDENTITY,
	tns_carrier_not_populated_cdrs decimal( 38 , 2) ,
	tns_carrier_not_populated_mou decimal(17, 2) 
) using delta ;



-- COMMAND ----------

INSERT INTO exc_CABS_no_AMA_Usage_Detail (
 
  date,
  tgn,
  item_no,
  recording_clli,
  far_clli,
  trunk_group_desc,
  traffic_desc,
  direction,
  ss7_peg_total,
  ss7_duration_total,
  ama_peg_total,
  ama_duration_total,
  inv_tgn,
  inv_item,
  inv_clli_ne,
  ivc_clli_fe,
  AMA_FLAG,
  RATE_CODE
)
SELECT 
  /*+ BROADCAST(b) */
  a.date,
  a.tgn,
  a.item_no,
  a.recording_clli,
  a.far_clli,
  a.trunk_group_desc,
  a.traffic_desc,
  a.direction,
  a.ss7_peg_total,
  a.ss7_duration_total,
  a.ama_peg_total,
  a.ama_duration_total,
  a.inv_tgn,
  a.inv_item,
  a.inv_clli_ne,
  a.ivc_clli_fe,
  a.AMA_FLAG,
  a.RATE_CODE
FROM v_tmp_tg_compare a
LEFT JOIN exc_CABS_no_AMA_Usage_Detail b
  ON a.date = b.date
  AND a.tgn = b.tgn
  AND a.item_no = b.item_no
  AND a.recording_clli = b.recording_clli
  AND a.far_clli = b.far_clli
  AND a.direction = b.direction
WHERE b.date IS NULL;


-- COMMAND ----------

DELETE FROM exc_CABS_no_AMA_Usage_Detail
WHERE date < DATE_SUB(CURRENT_DATE(), 11);


-- COMMAND ----------

DELETE FROM exc_CABS_no_AMA_Usage_Detail
WHERE ((ss7_duration_total - COALESCE(ama_duration_total, 0)) / ss7_duration_total) < 0.3;


-- COMMAND ----------

UPDATE exc_CABS_no_AMA_Usage_Detail
SET exception_row = NULL;


-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW v_tmp_exception_row AS
SELECT 
  item_no, rate_code, direction, trunk_group_desc, traffic_desc,
  COUNT(*) AS total_rows,
  ROW_NUMBER() OVER (
    ORDER BY item_no, rate_code, direction, trunk_group_desc, traffic_desc
  ) AS exception_row
FROM exc_CABS_no_AMA_Usage_Detail
GROUP BY item_no, rate_code, direction, trunk_group_desc, traffic_desc;


-- COMMAND ----------

MERGE INTO exc_CABS_no_AMA_Usage_Detail AS target
USING v_tmp_exception_row AS source
ON target.item_no = source.item_no
   AND target.rate_code = source.rate_code
   AND target.direction = source.direction
   AND target.trunk_group_desc = source.trunk_group_desc
   AND target.traffic_desc = source.traffic_desc
WHEN MATCHED THEN
  UPDATE SET target.exception_row = source.exception_row;


-- COMMAND ----------

CREATE OR REPLACE TABLE tmp_Item_rate_co_summary AS
SELECT DISTINCT 
  sub.exception_row, sub.min_date, sub.max_date, sub.item_no, sub.rate_code,
  sub.trunk_group_desc, sub.traffic_desc, sub.direction,
  sub.ss7_peg_total, sub.ss7_duration_total, sub.ama_peg_total, sub.ama_duration_total,
  sub.Total_TG_count, sub.TG_count_with_no_usage,
  b.tgn AS tgn_example, b.item_no AS item_no_example,
  b.recording_clli AS recording_clli_example, b.far_clli AS far_clli_example
FROM (
  SELECT 
    exception_row,
    MIN(date) AS min_date,
    MAX(date) AS max_date,
    item_no, rate_code, trunk_group_desc, traffic_desc, direction,
    SUM(ss7_peg_total) AS ss7_peg_total,
    SUM(ss7_duration_total) AS ss7_duration_total,
    SUM(COALESCE(ama_peg_total, 0)) AS ama_peg_total,
    SUM(COALESCE(ama_duration_total, 0)) AS ama_duration_total,
    COUNT(DISTINCT CONCAT(tgn, recording_clli, far_clli)) AS Total_TG_count,
    CAST(NULL AS INT) AS TG_count_with_no_usage,
    MIN(CASE WHEN ama_duration_total IS NULL THEN row_id ELSE NULL END) AS row_id_example
  FROM exc_CABS_no_AMA_Usage_Detail
  GROUP BY exception_row, item_no, rate_code, trunk_group_desc, traffic_desc, direction
) sub
LEFT JOIN exc_CABS_no_AMA_Usage_Detail b
  ON sub.row_id_example = b.row_id
ORDER BY sub.ss7_peg_total DESC;


-- COMMAND ----------

CREATE OR REPLACE TABLE tmp_TG_usage_counts AS
SELECT 
  exception_row,
  COUNT(DISTINCT CONCAT(tgn, recording_clli, far_clli)) AS TG_count_with_no_usage
FROM exc_CABS_no_AMA_Usage_Detail
WHERE ama_duration_total IS NULL
GROUP BY exception_row;


-- COMMAND ----------

MERGE INTO tmp_Item_rate_co_summary AS target
USING tmp_TG_usage_counts AS source
ON target.exception_row = source.exception_row
WHEN MATCHED THEN
  UPDATE SET target.TG_count_with_no_usage = source.TG_count_with_no_usage;


-- COMMAND ----------

CREATE OR REPLACE TABLE tmp_exceptions AS
SELECT DISTINCT 
  a.*, b.comment
FROM tmp_Item_rate_co_summary a
INNER JOIN (
  SELECT * 
  FROM ref_CABS_Item_Rate_Code_Rules
  WHERE ama_billable IS NULL OR ama_billable = 1
) b
ON COALESCE(a.item_no, 111) = COALESCE(b.item_no, 111)
   AND COALESCE(a.rate_code, 11) = COALESCE(b.rate_code, 11)
   AND a.direction = b.direction;


-- COMMAND ----------

DELETE FROM tmp_exceptions
WHERE TG_count_with_no_usage IS NULL;



-- COMMAND ----------


CREATE or replace  TABLE  exc_CABS_no_AMA_Usage(
	exception_row bigint ,
	min_date timestamp ,
	max_date timestamp ,
	ITEM_NO string ,
	rate_code string ,
	TRUNK_GROUP_DESC string ,
	TRAFFIC_DESC string ,
	direction string ,
	ss7_peg_total decimal(38 , 2) ,
	ss7_duration_total decimal(38, 6) ,
	ama_peg_total decimal(38 , 2) ,
	ama_duration_total decimal(38, 2) ,
	Total_TG_count int ,
	TG_count_with_no_usage int ,
	tgn_example string ,
	item_no_example string ,
	recording_clli_example string ,
	far_clli_example string ,
	comment string ,
	tns_carrier_not_populated_cdrs decimal(38 , 2) ,
	tns_carrier_not_populated_mou decimal(17, 2) 
) using delta ;



-- COMMAND ----------

INSERT INTO exc_CABS_no_AMA_Usage (
  
  exception_row,
  min_date,
  max_date,
  ITEM_NO,
  rate_code,
  TRUNK_GROUP_DESC,
  TRAFFIC_DESC,
  direction,
  ss7_peg_total,
  ss7_duration_total,
  ama_peg_total,
  ama_duration_total,
  Total_TG_count,
  TG_count_with_no_usage,
  tgn_example,
  item_no_example,
  recording_clli_example,
  far_clli_example,
  comment
)
SELECT DISTINCT
 
  exception_row,
  min_date,
  max_date,
  ITEM_NO,
  rate_code,
  TRUNK_GROUP_DESC,
  TRAFFIC_DESC,
  direction,
  ss7_peg_total,
  ss7_duration_total,
  ama_peg_total,
  ama_duration_total,
  Total_TG_count,
  TG_count_with_no_usage,
  tgn_example,
  item_no_example,
  recording_clli_example,
  far_clli_example,
  comment
FROM tmp_exceptions; 


-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW v_tmp_term_tns_carrier AS
SELECT 
  CAST(dateforstartofcharge AS DATE) AS date,
  item,
  rate_code,
  tgn,
  clli_ne,
  direction,
  SUM(cdrs) AS cdrs,
  SUM(mou) AS mou,
  SUM(CASE WHEN tns IS NULL OR carrier IS NULL THEN cdrs ELSE 0 END) AS tns_carrier_not_populated_cdrs,
  SUM(CASE WHEN tns IS NULL OR carrier IS NULL THEN mou ELSE 0 END) AS tns_carrier_not_populated_mou
FROM usg_SS7_Term
GROUP BY CAST(dateforstartofcharge AS DATE), item, rate_code, tgn, clli_ne, direction;


-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW v_tmp_orig_tns_carrier AS
SELECT 
  CAST(dateforstartofcharge AS DATE) AS date,
  item,
  rate_code,
  tgn,
  clli_ne,
  direction,
  SUM(cdrs) AS cdrs,
  SUM(mou) AS mou,
  SUM(CASE WHEN tns IS NULL OR carrier IS NULL THEN cdrs ELSE 0 END) AS tns_carrier_not_populated_cdrs,
  SUM(CASE WHEN tns IS NULL OR carrier IS NULL THEN mou ELSE 0 END) AS tns_carrier_not_populated_mou
FROM usg_SS7_Orig
GROUP BY CAST(dateforstartofcharge AS DATE), item, rate_code, tgn, clli_ne, direction;


-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW v_tmp_ss7_tns_carrier AS
SELECT * FROM v_tmp_term_tns_carrier
UNION ALL
SELECT * FROM v_tmp_orig_tns_carrier;


-- COMMAND ----------


CREATE or REPLACE TABLE TMP_ss7_tns_carrier( 
	date timestamp ,
	item string ,
	rate_code string ,
	tgn string,
	clli_ne string ,
	direction string ,
	cdrs decimal (38 , 2) ,
	mou decimal (38 , 2) ,
	tns_carrier_not_populated_cdrs decimal (38 , 2) ,
	tns_carrier_not_populated_mou decimal (38 , 2) 
) using delta ;


-- COMMAND ----------

INSERT INTO tmp_ss7_tns_carrier (
 
  date,
  item,
  rate_code,
  tgn,
  clli_ne,
  direction,
  cdrs,
  mou,
  tns_carrier_not_populated_cdrs,
  tns_carrier_not_populated_mou
)
SELECT 
  
  a.date,
  a.item,
  a.rate_code,
  a.tgn,
  a.clli_ne,
  a.direction,
  a.cdrs,
  a.mou,
  a.tns_carrier_not_populated_cdrs,
  a.tns_carrier_not_populated_mou
FROM v_tmp_ss7_tns_carrier a
LEFT JOIN tmp_ss7_tns_carrier b
  ON a.date = b.date
  AND COALESCE(a.item, 1) = COALESCE(b.item, 1)
  AND a.rate_code = b.rate_code
  AND COALESCE(a.tgn, '111') = COALESCE(b.tgn, '111')
  AND COALESCE(a.clli_ne, 'abc') = COALESCE(b.clli_ne, 'abc')
  AND a.direction = b.direction
WHERE b.tgn IS NULL AND b.clli_ne IS NULL AND b.direction IS NULL;


-- COMMAND ----------

DELETE FROM tmp_ss7_tns_carrier
WHERE date < (SELECT MIN(date) FROM exc_CABS_no_AMA_Usage_Detail);


-- COMMAND ----------

MERGE INTO exc_CABS_no_AMA_Usage_Detail AS target
USING tmp_ss7_tns_carrier AS source
ON target.date = source.date
   AND target.item_no = source.item
   AND target.rate_code = source.rate_code
   AND target.tgn = source.tgn
   AND target.recording_clli = source.clli_ne
   AND target.direction = source.direction
WHEN MATCHED THEN
  UPDATE SET
    target.tns_carrier_not_populated_cdrs = COALESCE(source.tns_carrier_not_populated_cdrs, 0),
    target.tns_carrier_not_populated_mou = COALESCE(source.tns_carrier_not_populated_mou, 0);


-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW v_tmp_totals_for_summary_table AS
SELECT 
  exception_row,
  SUM(tns_carrier_not_populated_cdrs) AS tns_carrier_not_populated_cdrs,
  SUM(tns_carrier_not_populated_mou) AS tns_carrier_not_populated_mou
FROM exc_CABS_no_AMA_Usage_Detail
GROUP BY exception_row;


-- COMMAND ----------

MERGE INTO exc_CABS_no_AMA_Usage AS target
USING v_tmp_totals_for_summary_table AS source
ON target.exception_row = source.exception_row
WHEN MATCHED THEN
  UPDATE SET
    target.tns_carrier_not_populated_cdrs = source.tns_carrier_not_populated_cdrs,
    target.tns_carrier_not_populated_mou = source.tns_carrier_not_populated_mou;


-- COMMAND ----------

-- Create a temporary view equivalent to #Incollect_Summary
CREATE OR REPLACE TABLE tmp_Incollect_Summary AS
SELECT 
  CAST(dateforstartofcharge AS DATE) AS dateforstartofcharge,
  toll_center,
  CAST(NULL AS STRING) AS COMPANY,
  COMPANY_NUMBER,
  direction,
  ixc,
  subcic,
  cdrs,
  mou,
  job_id
FROM (
  SELECT 
    CAST(dateforstartofcharge AS DATE) AS dateforstartofcharge,
    toll_center,
    COMPANY_NUMBER,
    direction,
    ixc,
    subcic,
    SUM(cdrs) AS cdrs,
    SUM(mou) AS mou,
    job_id
  FROM usg_emi_orig
  GROUP BY 
    CAST(dateforstartofcharge AS DATE),
    toll_center,
    COMPANY_NUMBER,
    direction,
    ixc,
    subcic,
    job_id

  UNION ALL

  SELECT 
    CAST(dateforstartofcharge AS DATE) AS dateforstartofcharge,
    toll_center,
    COMPANY_NUMBER,
    direction,
    ixc,
    subcic,
    SUM(cdrs) AS cdrs,
    SUM(mou) AS mou,
    job_id
  FROM usg_emi_term
  GROUP BY 
    CAST(dateforstartofcharge AS DATE),
    toll_center,
    COMPANY_NUMBER,
    direction,
    ixc,
    subcic,
    job_id
) sub;

-- COMMAND ----------

CREATE OR REPLACE table tmp_toll_centers AS 
SELECT DISTINCT toll_center, company, states, toll_cabs, rds_report
FROM ref_cabs_incollect_toll_centers; ---chk


-- COMMAND ----------

DELETE FROM tmp_toll_centers
WHERE toll_cabs = 'TOLL'
   OR LENGTH(company) != (
     SELECT MAX(LENGTH(company))
     FROM tmp_toll_centers AS b
     WHERE tmp_toll_centers.toll_center = b.toll_center
   )
   OR company != (
     SELECT MIN(company)
     FROM tmp_toll_centers AS b
     WHERE tmp_toll_centers.toll_center = b.toll_center
   );


-- COMMAND ----------

MERGE INTO tmp_Incollect_Summary AS target
USING tmp_toll_centers AS source
ON target.toll_center = source.toll_center
WHEN MATCHED AND target.COMPANY IS NULL THEN
  UPDATE SET target.COMPANY = source.company

-- COMMAND ----------

CREATE or REPLACE TABLE stg_CABS_Incollect_Toll_Center_Summary(
	dateforstartofcharge timestamp ,
	toll_center string ,
	COMPANY string ,
	COMPANY_NUMBER string ,
	direction string ,
	ixc string ,
	subcic string ,
	cdrs decimal( 38 , 2) ,
	mou decimal( 38 , 2) ,
	job_id bigint 
) using delta ;

-- COMMAND ----------

DELETE FROM stg_CABS_Incollect_Toll_Center_Summary
WHERE EXISTS (
  SELECT 1
  FROM tmp_Incollect_Summary t
  WHERE stg_CABS_Incollect_Toll_Center_Summary.dateforstartofcharge = t.dateforstartofcharge
    AND stg_CABS_Incollect_Toll_Center_Summary.direction = t.direction
    AND stg_CABS_Incollect_Toll_Center_Summary.job_id = t.job_id
);

-- COMMAND ----------

INSERT INTO stg_CABS_Incollect_Toll_Center_Summary (
  dateforstartofcharge,
  toll_center,
  company,
  company_number,
  direction,
  ixc,
  subcic,
  cdrs,
  mou,
  job_id
)
SELECT /*+ BROADCAST(b) */ DISTINCT
  a.dateforstartofcharge,
  a.toll_center,
  a.company,
  a.company_number,
  a.direction,
  a.ixc,
  a.subcic,
  a.cdrs,
  a.mou,
  a.job_id
FROM tmp_Incollect_Summary a
LEFT JOIN stg_CABS_Incollect_Toll_Center_Summary b
  ON a.dateforstartofcharge = b.dateforstartofcharge
  AND a.direction = b.direction
  AND a.job_id = b.job_id
WHERE b.dateforstartofcharge IS NULL;

-- COMMAND ----------

-- this is used for optimization purpose , to find the size od the table . no need to run again
--DESCRIBE DETAIL stg_CABS_Incollect_Toll_Center_Summary;
--DESCRIBE DETAIL tmp_ss7_tns_carrier;

-- COMMAND ----------

DELETE FROM stg_CABS_Incollect_Toll_Center_Summary
WHERE dateforstartofcharge < DATE_SUB(CURRENT_DATE(), 95);


-- COMMAND ----------

CREATE or REPLACE TABLE usg_CABS_Incollect_Toll_Center_Summary(
	dateforstartofcharge timestamp ,
	toll_center string  ,
	company string ,
	company_number string  ,
	direction string  ,
	ixc string  ,
	subcic string  ,
	cdrs decimal ( 38 , 2) ,
	mou decimal ( 38 , 2) 
) using delta ;

-- COMMAND ----------


INSERT INTO usg_CABS_Incollect_Toll_Center_Summary (
  dateforstartofcharge,
  toll_center,
  company,
  company_number,
  direction,
  ixc,
  subcic,
  cdrs,
  mou
)
SELECT

  dateforstartofcharge,
  toll_center,
  company,
  company_number,
  direction,
  ixc,
  subcic,
  SUM(cdrs),
  SUM(mou)
FROM stg_CABS_Incollect_Toll_Center_Summary
GROUP BY dateforstartofcharge, toll_center, company, company_number, direction, ixc, subcic;


-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW v_tmp_monthly_incollect_summary AS
SELECT 
  DATE_TRUNC('month', dateforstartofcharge) AS month,
  MIN(dateforstartofcharge) AS min_date,
  MAX(dateforstartofcharge) AS max_date,
  toll_center,
  company,
  company_number,
  direction,
  ixc,
  subcic,
  SUM(cdrs) AS cdrs,
  SUM(mou) AS mou
FROM usg_CABS_Incollect_Toll_Center_Summary
GROUP BY DATE_TRUNC('month', dateforstartofcharge), toll_center, company, company_number, direction, ixc, subcic;


-- COMMAND ----------

CREATE or replace table usg_CABS_Incollect_Toll_Center_Monthly_Summary(
	month timestamp ,
	min_date timestamp ,
	max_date timestamp ,
	toll_center string ,
	company string,
	company_number string ,
	direction string ,
	ixc string ,
	subcic string ,
	cdrs decimal( 38 , 2) ,
	mou decimal( 38 , 2) 
) using delta ;

-- COMMAND ----------

DELETE FROM usg_CABS_Incollect_Toll_Center_Monthly_Summary
WHERE month >= DATE_TRUNC('month', ADD_MONTHS(CURRENT_DATE(), -1));


-- COMMAND ----------

INSERT INTO usg_CABS_Incollect_Toll_Center_Monthly_Summary (
  
  month,
  min_date,
  max_date,
  toll_center,
  company,
  company_number,
  direction,
  ixc,
  subcic,
  cdrs,
  mou
)
SELECT 
 
  a.month,
  a.min_date,
  a.max_date,
  a.toll_center,
  a.company,
  a.company_number,
  a.direction,
  a.ixc,
  a.subcic,
  a.cdrs,
  a.mou
FROM v_tmp_monthly_incollect_summary a
LEFT JOIN (
  SELECT DISTINCT month FROM usg_CABS_Incollect_Toll_Center_Monthly_Summary
) b
ON a.month = b.month
WHERE b.month IS NULL;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Performance Monitoring and Statistics
-- MAGIC Monitor performance and update statistics

-- COMMAND ----------

--DROP TABLE IF EXISTS tmp_Item_rate_co_summary;
--DROP TABLE IF EXISTS tmp_TG_usage_counts;
--DROP TABLE IF EXISTS tmp_exceptions;
----DROP TABLE IF EXISTS TMP_ss7_tns_carrier;
--DROP TABLE IF EXISTS tmp_Incollect_Summary;
--DROP TABLE IF EXISTS tmp_toll_centers;

-- COMMAND ----------

-- OPTIMIZED: Table optimization with better column ordering
OPTIMIZE usg_rt_CABS_CO_NBR_IXC_Usage_Summary ZORDER BY (dateforstartofcharge, direction, COMPANY_NUMBER, ixc, job_id);

OPTIMIZE usg_CABS_SS7_MOU_EMI_Daily_Totals ZORDER BY (dateforstartofcharge);

OPTIMIZE usg_rt_CABS_Usage_Summary ZORDER BY (dateforstartofcharge, direction, job_id);

OPTIMIZE exc_CABS_no_AMA_Usage_Detail ZORDER BY (date, item_no, rate_code, tgn, recording_clli, direction);

OPTIMIZE stg_CABS_Incollect_Toll_Center_Summary ZORDER BY (dateforstartofcharge, toll_center, company, company_number, direction, ixc, subcic);

OPTIMIZE usg_CABS_Incollect_Toll_Center_Summary ZORDER BY (dateforstartofcharge, toll_center, company, company_number, direction, ixc, subcic);

OPTIMIZE usg_CABS_Incollect_Toll_Center_Monthly_Summary ZORDER BY (month, toll_center, company, company_number, direction, ixc, subcic);


-- Update table statistics for better query planning

ANALYZE TABLE usg_rt_CABS_CO_NBR_IXC_Usage_Summary COMPUTE STATISTICS;
ANALYZE TABLE usg_CABS_SS7_MOU_EMI_Daily_Totals COMPUTE STATISTICS;
ANALYZE TABLE usg_rt_CABS_Usage_Summary COMPUTE STATISTICS;
ANALYZE TABLE exc_CABS_no_AMA_Usage COMPUTE STATISTICS;
ANALYZE TABLE usg_AMA_for_Load_Summary COMPUTE STATISTICS;
ANALYZE TABLE exc_CABS_no_AMA_Usage_Detail COMPUTE STATISTICS;
ANALYZE TABLE stg_CABS_Incollect_Toll_Center_Summary COMPUTE STATISTICS;
ANALYZE TABLE usg_CABS_Incollect_Toll_Center_Summary COMPUTE STATISTICS;
ANALYZE TABLE usg_CABS_Incollect_Toll_Center_Monthly_Summary COMPUTE STATISTICS;



-- COMMAND ----------

--  Optimizations
--  Used common table expressions (CTEs) and temporary views to simplify queries and avoid repeating logic.
--  Used ZORDER for better data organization
--  Replaced DELETE/INSERT with MERGE for better performance
--  Updated table statistics for better query planning
