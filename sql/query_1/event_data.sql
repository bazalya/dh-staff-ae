/*
 *  This table unnests the event_variables and pivots them into columns
 *
 *  UPDATE: incremental by partition_date
 *  PK: surrogate key from session_id, event_name, experiment_variation, experiment_id
 */

CREATE TABLE `dh-codapro-analytics-2460.hiring_search_analytics.event_data`
PARTITION BY partition_date AS (
  WITH behavioural_data AS (
    SELECT
      behavioural_data.* EXCEPT (event_variables)
      , event_variables.name
      , event_variables.value
    FROM `dh-codapro-analytics-2460.hiring_search_analytics.behavioural_customer_data` AS behavioural_data
    LEFT JOIN UNNEST(event_variables) AS event_variables
    WHERE behavioural_data.device != 'Backend' -- assuming this is a test
  )

  -- DISTINCT gets rid of all the duplicate experiment variations with different event_ids related to the same session_id
  -- Those events aren't needed as we're not looking at each individual event
  SELECT DISTINCT * EXCEPT (event_id)
  FROM behavioural_data
  PIVOT (
    MAX(value) FOR name IN (
      'experimentVariation' AS experiment_variation
      , 'experimentId' AS experiment_id
      , 'deliveryAddressStatus' AS delivery_address_status
      , 'shopCountry' AS shop_country
    )
  )
  WHERE NOT (
    event_name = 'experiment.participated'
    AND COALESCE(experiment_variation, '') = '' -- assuming '' is an erroneous experiment_variation identifier
    AND COALESCE(experiment_id, '') = '' -- assuming '' is an erroneous experiment_id identifier
  )
);
