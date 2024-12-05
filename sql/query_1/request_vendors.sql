/*
 *  This table contains the vendor data from the backend logging data per request_id
 *
 *  UPDATE: incremental by partition_date
 *  PK: surrogate key from all values
 */

CREATE TABLE `dh-codapro-analytics-2460.hiring_search_analytics.request_vendors`
PARTITION BY partition_date AS (
  SELECT
    backend_data.partition_date
    , backend_data.request_id
    , backend_data.response_id
    , backend_data.perseus_id                                               AS client_id
    , backend_data.perseus_session_id                                       AS session_id
    , JSON_EXTRACT_SCALAR(vendors, "$.id")                                  AS vendor_id
    , JSON_EXTRACT_SCALAR(vendors, "$.delivery_info.delivery_fee")          AS vendor_delivery_fee
    , JSON_EXTRACT_SCALAR(vendors, "$.delivery_info.delivery_time_minutes") AS vendor_delivery_time_minutes
    , JSON_EXTRACT_SCALAR(vendors, "$.score")                               AS vendor_score
    , JSON_EXTRACT_SCALAR(vendors, "$.status")                              AS vendor_status
  FROM `dh-codapro-analytics-2460.hiring_search_analytics.backend_logging_data` AS backend_data
  , UNNEST(JSON_EXTRACT_ARRAY(payload, "$.vendors")) AS vendors
);
