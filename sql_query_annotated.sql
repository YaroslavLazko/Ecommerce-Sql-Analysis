-- Final SQL query for E-commerce Advanced Analysis
-- Goal: Build a dataset that combines account creation
--       and email activity metrics, grouped by key
--       dimensions (date, country, send_interval, verification, subscription).
--       Also calculate country-level totals and ranks,
--       keeping only Top-10 countries.
-- ================================================

-- Step 1: Create a table with account-level metrics
-- For each date, country, send interval, verification & unsubscribe status
-- we count distinct accounts created.
WITH acc_table AS (
  SELECT s.date,
        sp.country,
        send_interval,
        ac.is_verified,
        ac.is_unsubscribed,
        COUNT (DISTINCT acs.account_id) AS ac_cn
  FROM `DA.account` AS ac
  JOIN `DA.account_session` AS  acs
  ON ac.id = acs.account_id
  JOIN `DA.session` AS s
  ON acs.ga_session_id = s.ga_session_id
  JOIN `DA.session_params` AS sp
  ON s.ga_session_id = sp.ga_session_id
  GROUP BY 1, 2, 3, 4, 5
),

-- Step 2: Create a table with email-related metrics
-- For each account dimension, we calculate:
-- - sent_ms  : number of sent messages
-- - open_ms  : number of opened messages
-- - visit_ms : number of visits from email
-- Note: We join with account/session tables to keep same breakdown dimensions.
mail_acc_table AS
(
  SELECT DATE_ADD (s.date, INTERVAL sent_date DAY) AS date,
        sp.country,
        ac.send_interval,
        ac.is_verified,
        ac.is_unsubscribed,
        COUNT (DISTINCT es.id_message) AS sent_ms,
        COUNT (DISTINCT eo.id_message) AS open_ms,
        COUNT (DISTINCT ev.id_message) AS visit_ms
  FROM `DA.email_sent` AS es
  LEFT JOIN `DA.email_open` AS eo
  ON es.id_message = eo.id_message
  LEFT JOIN `DA.email_visit` AS ev
  ON es.id_message = ev.id_message
  JOIN `DA.account` AS ac
  ON ac.id = es.id_account
  JOIN `DA.account_session` AS  acs
  ON ac.id = acs.account_id
  JOIN `DA.session` AS s
  ON acs.ga_session_id = s.ga_session_id
  JOIN `DA.session_params` AS sp
  ON sp.ga_session_id = s.ga_session_id
  GROUP BY 1, 2, 3, 4, 5
),

-- Step 3: Combine account and email tables using UNION
-- Important: Keep account and email metrics separated by filling
--            zeros for missing fields.
union_table as
(
  -- Account metrics (no emails)
  SELECT date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        ac_cn,
        0 as sent_ms,
        0 as open_ms,
        0 as visit_ms
  FROM acc_table
  
  UNION ALL
  
  -- Email metrics (no new accounts)
  SELECT date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        0 as ac_cn,
        sent_ms,
        open_ms,
        visit_ms
  FROM mail_acc_table
),

-- Step 4: Aggregate both parts together
-- Summing accounts and email activity metrics
-- so that we get one combined dataset per category.
agregate_table AS
(
  SELECT date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        SUM (ac_cn) AS ac_cnt,
        SUM (sent_ms) AS sent_msg,
        SUM (open_ms) AS open_msg,
        SUM (visit_ms) AS visit_msg
  FROM union_table
  GROUP BY 1, 2, 3, 4, 5
),

-- Step 5: Add window functions
-- Calculate totals per country:
-- - total_country_account_cnt = total accounts by country
-- - total_country_sent_cnt    = total sent emails by country
wind_table as
(
  SELECT *,
        SUM (ac_cnt) OVER (PARTITION BY country) AS total_country_account_cnt,
        SUM (sent_msg) OVER (PARTITION BY country) AS total_country_sent_cnt,
  FROM agregate_table
)

-- Step 6: Apply ranking logic and filter Top-10 countries
-- Using DENSE_RANK:
-- - rank_total_country_account_cnt = ranking by accounts
-- - rank_total_country_sent_cnt    = ranking by sent emails
-- Final dataset keeps only top 10 countries by either accounts or sent messages.
SELECT *,
        DENSE_RANK () OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
        DENSE_RANK () OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
FROM wind_table
QUALIFY rank_total_country_account_cnt <= 10 OR rank_total_country_sent_cnt <= 10
