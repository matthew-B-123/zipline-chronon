from gen_thrift.api.ttypes import EntitySource, Source, Accuracy
from ai.chronon.group_by import GroupBy
from ai.chronon.query import Query, selects

# Source: Read from the Iceberg table created by staging query
# The staging query creates: affirm.affirm_paybright_repayment_data_v1_0_sq_v1__1
source = Source(
    entities=EntitySource(
        snapshotTable="affirm.affirm_paybright_repayment_data_v1_0_sq_v1__1",
        query=Query(
            selects=selects(
                "user_phone_number",
                "snapshot_time",
                "processed_time",
                "galactus__paybright__user__history_average_lateness_days__v1",
                "galactus__paybright__user__history_average_zeroed_lateness_days__v1",
                "galactus__paybright__user__history_days_since_last_payment__v1",
                "galactus__paybright__user__history_max_lateness_days__v1",
                "galactus__paybright__user__history_num_outstanding_loans__v1",
                "galactus__paybright__user__history_num_payments_last_60d__v1",
                "galactus__paybright__user__history_prop_fully_paid_off_loans__il__v1",
                "galactus__paybright__user__history_prop_fully_paid_off_loans__sp__v1",
                "galactus__paybright__user__history_total_payment_amount_cents_60d__v1",
                "galactus__paybright__user__history_total_payment_amount_cents__v1",
            )
        )
    )
)

# GroupBy with no aggregations (pass-through of snapshot data)
v1 = GroupBy(
    sources=[source],
    keys=["user_phone_number"],
    aggregations=[],  # Pass-through - no aggregations
    accuracy=Accuracy.SNAPSHOT,  # Daily snapshots
    online=True,  # Enable online serving
    version=1,
    backfill_start_date="2023-11-07",  # Match our staging query data
)

