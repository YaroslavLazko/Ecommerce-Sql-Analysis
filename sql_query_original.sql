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
  left join `DA.email_open` AS eo
  ON es.id_message = eo.id_message
  LEFT JOIN `DA.email_visit` AS ev
  ON es.id_message = ev.id_message
  join `DA.account` AS ac
  ON ac.id = es.id_account
  JOIN `DA.account_session` AS  acs
  ON ac.id = acs.account_id
  JOIN `DA.session` AS s
  ON acs.ga_session_id = s.ga_session_id
  JOIN `DA.session_params` AS sp
  ON sp.ga_session_id = s.ga_session_id
  GROUP BY 1, 2, 3, 4, 5
),
union_table as
(
  SELECT date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        ac_cn,
        0 as sent_ms,
        0 as open_ms,
        0 as visit_ms
  from acc_table
  union all
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
wind_table as
(
  SELECT *,
        SUM (ac_cnt) OVER (PARTITION BY country) AS total_country_account_cnt,
        SUM (sent_msg) OVER (PARTITION BY country) AS total_country_sent_cnt,
  from agregate_table
)
  SELECT *,
        DENSE_RANK () OVER (ORDER BY total_country_account_cnt desc) AS rank_total_country_account_cnt,
        DENSE_RANK () OVER (ORDER BY total_country_sent_cnt desc) AS rank_total_country_sent_cnt
  FROM wind_table
  QUALIFY rank_total_country_account_cnt <= 10 OR rank_total_country_sent_cnt <= 10
