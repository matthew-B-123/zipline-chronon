# /tmp/paybright_repayment_data_v1_0_sq.py
from ai.chronon.staging_query import StagingQuery

# Real S3 path - ALL DATES (wildcards for YYYY/MM/DD)
S3_PREFIX = "s3a://affirm-risk-sherlock-ca/feature-store/paybright_repayment_data/v1/*/*/*/out/"

v1 = StagingQuery(
    setups=[
        # Create a temporary view over the parquet files
        f"CREATE OR REPLACE TEMPORARY VIEW paybright_raw USING parquet OPTIONS (path '{S3_PREFIX}')"
    ],
    query="""
      WITH src AS (
        SELECT
          /* Keys & timestamps - handle schema evolution (old: 'created', new: 'snapshot_time'/'processed_time') */
          CAST(user_phone_number AS STRING) AS user_phone_number,
          CAST(COALESCE(snapshot_time, created) AS TIMESTAMP) AS snapshot_time,
          CAST(COALESCE(processed_time, created) AS TIMESTAMP) AS processed_time,

          /* Features (cast for stability) */
          CAST(galactus__paybright__user__history_average_lateness_days__v1           AS DOUBLE)
            AS galactus__paybright__user__history_average_lateness_days__v1,
          CAST(galactus__paybright__user__history_average_zeroed_lateness_days__v1    AS DOUBLE)
            AS galactus__paybright__user__history_average_zeroed_lateness_days__v1,
          CAST(galactus__paybright__user__history_days_since_last_payment__v1         AS DOUBLE)
            AS galactus__paybright__user__history_days_since_last_payment__v1,
          CAST(galactus__paybright__user__history_max_lateness_days__v1               AS DOUBLE)
            AS galactus__paybright__user__history_max_lateness_days__v1,
          CAST(galactus__paybright__user__history_num_outstanding_loans__v1           AS DOUBLE)
            AS galactus__paybright__user__history_num_outstanding_loans__v1,
          CAST(galactus__paybright__user__history_num_payments_last_60d__v1           AS DOUBLE)
            AS galactus__paybright__user__history_num_payments_last_60d__v1,
          CAST(galactus__paybright__user__history_prop_fully_paid_off_loans__il__v1   AS DOUBLE)
            AS galactus__paybright__user__history_prop_fully_paid_off_loans__il__v1,
          CAST(galactus__paybright__user__history_prop_fully_paid_off_loans__sp__v1   AS DOUBLE)
            AS galactus__paybright__user__history_prop_fully_paid_off_loans__sp__v1,
          CAST(galactus__paybright__user__history_total_payment_amount_cents_60d__v1  AS DOUBLE)
            AS galactus__paybright__user__history_total_payment_amount_cents_60d__v1,
          CAST(galactus__paybright__user__history_total_payment_amount_cents__v1      AS DOUBLE)
            AS galactus__paybright__user__history_total_payment_amount_cents__v1,

          /* optional event-time in ms (if needed later) */
          CAST(unix_timestamp(snapshot_time) * 1000 AS BIGINT) AS ts,

          /* Derive ds from snapshot_time (avoid brittle path regex) */
          TO_DATE(snapshot_time) AS ds
        FROM paybright_raw
        WHERE user_phone_number IS NOT NULL
      )
      SELECT * FROM src
    """,
    version=1,
    output_namespace="affirm",      # database name (catalog comes from your Spark env)
    dependencies=[],
    table_properties={
        "provider": "iceberg",
        "write.format.default": "parquet"
    }
)
