/*
 *  This table summarizes the session data
 *  total_order_value_eur is assumed to be the sum of order_value_eur, discount_value_eur and voucher_value_eur.
 *
 *  UPDATE: incremental by partition_date
 *  PK: session_id
 */

CREATE TABLE `dh-codapro-analytics-2460.hiring_search_analytics.session_summary`
PARTITION BY partition_date AS (
  WITH customer_contact AS (
    SELECT DISTINCT order_id
    FROM `dh-codapro-analytics-2460.hiring_search_analytics.orders_customer_contact` -- created from orders_customer_contact.sql
  )

  , orders AS (
    SELECT
      orders.*
      , IF(customer_contact.order_id IS NOT NULL, TRUE, FALSE) AS has_customer_contact
    FROM `dh-codapro-analytics-2460.hiring_search_analytics.customer_orders_data` AS orders
    LEFT JOIN customer_contact
      ON orders.order_id = customer_contact.order_id
  )

  , backend_data AS (
    SELECT
      session_id
      , brand
      , sub_brand
      , global_entity_id
      , country_code
      , language_code
      , COUNT(DISTINCT request_id)                                                     AS request_query_count
      , COUNT(DISTINCT vertical_type)                                                  AS session_vertical_types_count
      , SUM(vendors_loaded_count)                                                      AS total_vendors_loaded_count
      , SAFE_DIVIDE(SUM(vendors_loaded_open_count), SUM(vendors_loaded_count)) * 100.0 AS total_vendors_loaded_open_percent
      , is_pro_customer
      , fwfc_experiment_key
      , fwfc_experiment_variation
      , fwfc_experiment_variation_name
      , fwfc_experiment_abtest
      , fwfc_experiment_explanation
      , fwfc_experiment_relevant_context
    FROM `dh-codapro-analytics-2460.hiring_search_analytics.backend_data` -- created from backend_data.sql
    GROUP BY ALL
  )

  SELECT
    event_data.partition_date
    , event_data.session_id
    , event_data.client_id
    , event_data.device
    , COALESCE(event_data.global_entity_id, backend_data.global_entity_id) AS global_entity_id
    , COALESCE(event_data.brand, backend_data.brand)                       AS brand
    , backend_data.sub_brand
    , backend_data.country_code
    , backend_data.language_code
    , backend_data.request_query_count
    , backend_data.session_vertical_types_count
    , backend_data.total_vendors_loaded_count
    , backend_data.total_vendors_loaded_open_percent
    , backend_data.is_pro_customer
    , backend_data.fwfc_experiment_key
    , backend_data.fwfc_experiment_variation
    , backend_data.fwfc_experiment_variation_name
    , backend_data.fwfc_experiment_abtest
    , backend_data.fwfc_experiment_explanation
    , backend_data.fwfc_experiment_relevant_context
    , SUM(IF(orders.is_successful, orders.order_value_eur, 0))             AS order_value_eur
    , SUM(
      IF(
        orders.is_successful
        , COALESCE(orders.order_value_eur, 0) + COALESCE(orders.discount_value_eur, 0) + COALESCE(orders.voucher_value_eur, 0)
        , 0
      )
    )                                                                      AS total_order_value_eur
    , COUNT(DISTINCT IF(orders.is_successful, orders.order_id, NULL))      AS successful_orders_count
    , COUNT(DISTINCT IF(orders.is_failed, orders.order_id, NULL))          AS failed_orders_count
    , MAX(COALESCE(orders.has_customer_contact, FALSE))                    AS has_customer_contact
  FROM `dh-codapro-analytics-2460.hiring_search_analytics.event_data` AS event_data -- created from event_data.sql
  INNER JOIN backend_data
    ON event_data.session_id = backend_data.session_id
  LEFT JOIN orders
    ON event_data.transaction_id = orders.order_id
  GROUP BY ALL
);
