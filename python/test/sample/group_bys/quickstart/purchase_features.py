"""
GroupBy for purchase features using the Iceberg table we just created.
Computes rolling aggregations and outputs to DynamoDB for online serving.
"""

from gen_thrift.api.ttypes import EventSource, Source
from ai.chronon.group_by import Aggregation, GroupBy, Operation
from ai.chronon.query import Query, selects

# Source: Read from the Iceberg table we created with staging query
source = Source(
    events=EventSource(
        table="quickstart.quickstart_purchases_from_minio_v1__1",  # Our Iceberg table from staging query
        query=Query(
            selects=selects(
                "user_id",
                "purchase_price",
                "category",
            ),
            time_column="ts"
        )
    )
)

# Time windows for rolling aggregations (simple string format)
windows = ["1d", "7d", "30d"]

# Create GroupBy with aggregations
v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    online=True,  # Enable online serving to DynamoDB
    version=1,
    backfill_start_date="2023-12-01",  # Start date for backfill
    aggregations=[
        # Total purchase amount
        Aggregation(
            input_column="purchase_price",
            operation=Operation.SUM,
            windows=windows
        ),
        # Purchase count
        Aggregation(
            input_column="purchase_price",
            operation=Operation.COUNT,
            windows=windows
        ),
        # Average purchase amount
        Aggregation(
            input_column="purchase_price",
            operation=Operation.AVERAGE,
            windows=windows
        ),
        # Max purchase amount
        Aggregation(
            input_column="purchase_price",
            operation=Operation.MAX,
            windows=windows
        ),
    ],
)

