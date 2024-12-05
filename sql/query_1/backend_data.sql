/*
 *  This table contains the backend logging data with the payload data flattened
 *
 *  UPDATE: incremental by partition_date
 *  PK: request_id
 */

CREATE TABLE `dh-codapro-analytics-2460.hiring_search_analytics.backend_data`
PARTITION BY partition_date AS (
  WITH request_vendors AS (
    SELECT
      request_id
      , COUNTIF(vendor_status = "open") AS vendors_loaded_open_count
      , COUNT(*)                        AS vendors_loaded_count
    FROM `dh-codapro-analytics-2460.hiring_search_analytics.request_vendors` -- created from request_vendors.sql
    GROUP BY ALL
  )

  SELECT
    backend_data.partition_date
    , backend_data.perseus_session_id                                             AS session_id
    , backend_data.perseus_id                                                     AS client_id
    , backend_data.request_id
    , backend_data.response_id
    , backend_data.brand
    , JSON_EXTRACT_SCALAR(backend_data.payload, "$.brand")                        AS sub_brand
    , backend_data.global_entity_id
    , UPPER(JSON_EXTRACT_SCALAR(backend_data.payload, "$.country_code"))          AS country_code
    , JSON_EXTRACT_SCALAR(backend_data.payload, "$.language_code")                AS language_code
    , JSON_EXTRACT_SCALAR(backend_data.payload, "$.query")                        AS search_query
    , JSON_EXTRACT_SCALAR(backend_data.payload, "$.sort.by")                      AS search_results_sort_by
    , JSON_EXTRACT_SCALAR(backend_data.payload, "$.sort.order")                   AS search_results_sort_order
    , JSON_EXTRACT_SCALAR(backend_data.payload, "$.vertical_types[0]")            AS vertical_type
    , request_vendors.vendors_loaded_count
    , request_vendors.vendors_loaded_open_count
    , JSON_EXTRACT_SCALAR(backend_data.payload, "$.pro_customer")                 AS is_pro_customer
    , backend_data.fun_with_flags_client.response.experiments[0].key              AS fwfc_experiment_key
    , backend_data.fun_with_flags_client.response.experiments[0].variation        AS fwfc_experiment_variation
    , backend_data.fun_with_flags_client.response.experiments[0].variation_name   AS fwfc_experiment_variation_name
    , backend_data.fun_with_flags_client.response.experiments[0].abtest           AS fwfc_experiment_abtest
    , backend_data.fun_with_flags_client.response.experiments[0].explanation      AS fwfc_experiment_explanation
    , backend_data.fun_with_flags_client.response.experiments[0].relevant_context AS fwfc_experiment_relevant_context
  FROM `dh-codapro-analytics-2460.hiring_search_analytics.backend_logging_data` AS backend_data
  LEFT JOIN request_vendors
    ON backend_data.request_id = request_vendors.request_id
);
