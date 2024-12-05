/*
 *  SESSION LEVEL
 *
 *  This query calculates the conversion rate and total order value for each experiment_variation. If the session_id
 *  ended up having a transaction_id with a successful order.
 *
 *  UPDATE: incremental by report_date
 */

SELECT
  CURRENT_DATE                                                           AS report_date
  , fwfc_experiment_variation                                            AS experiment_variation
  , COALESCE(country_code, RIGHT(global_entity_id, 2))                   AS region
  , device
  , COUNT(session_id)                                                    AS sessions_count
  , SAFE_DIVIDE(COUNTIF(successful_orders_count > 0), COUNT(session_id)) AS conversion_rate
  , SUM(successful_orders_count)                                         AS successful_orders_count
  , SUM(total_order_value_eur)                                           AS total_order_value_eur
FROM `dh-codapro-analytics-2460.hiring_search_analytics.session_summary` -- created from session_summary.sql
GROUP BY ALL;

/*
 *  EVENT LEVEL
 *
 *  Since for one session_id there could be multiple experiment_variations based on events, the and total order value and
 *  a conversion are attributed to all experiment_variations attributed to the same session_id.
 *
 *  UPDATE: incremental by report_date
 */

SELECT
  CURRENT_DATE                                                                    AS report_date
  , event_data.experiment_variation
  , COALESCE(session_summary.country_code, RIGHT(event_data.global_entity_id, 2)) AS region
  , COALESCE(session_summary.device, event_data.device)                           AS device
  , COUNT(DISTINCT session_summary.session_id)                                    AS sessions_count
  , SAFE_DIVIDE(
    COUNT(DISTINCT IF(session_summary.successful_orders_count > 0, session_summary.session_id, NULL))
    , COUNT(DISTINCT session_summary.session_id)
  )                                                                               AS conversion_rate
  , SUM(session_summary.successful_orders_count)                                  AS successful_orders_count
  , SUM(session_summary.total_order_value_eur)                                    AS total_order_value_eur
FROM `dh-codapro-analytics-2460.hiring_search_analytics.event_data` AS event_data -- created from event_data.sql
INNER JOIN `dh-codapro-analytics-2460.hiring_search_analytics.session_summary` AS session_summary -- created from session_summary.sql
  ON event_data.session_id = session_summary.session_id
WHERE event_data.experiment_variation IS NOT NULL
GROUP BY ALL;
