/*
 *  This table contains the customer contact data from the orders data per order_id
 *
 *  UPDATE: incremental by partition_date
 */

CREATE TABLE `dh-codapro-analytics-2460.hiring_search_analytics.orders_customer_contact`
PARTITION BY partition_date AS (
  WITH session_ids AS (
    SELECT DISTINCT
      session_id
      , transaction_id
    FROM `dh-codapro-analytics-2460.hiring_search_analytics.behavioural_customer_data`
    WHERE transaction_id IS NOT NULL
  )

  SELECT
    orders.order_id
    , session_ids.session_id
    , orders.partition_date
    , customer_contact.contact_at
    , customer_contact.reason
  FROM `dh-codapro-analytics-2460.hiring_search_analytics.customer_orders_data` AS orders
  LEFT JOIN UNNEST(customer_contact) AS customer_contact
  LEFT JOIN session_ids
    ON orders.order_id = session_ids.transaction_id
  WHERE NOT (
    customer_contact.contact_at IS NULL
    AND customer_contact.reason IS NULL
  )
);
