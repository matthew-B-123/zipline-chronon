#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from ai.chronon.staging_query import StagingQuery

# Staging query that reads Parquet from MinIO S3 and writes to table
# S3 path: s3a://chronon/warehouse/data/purchases/purchases.parquet
# No date filtering - loads all data
query = """
SELECT
    user_id,
    UNIX_TIMESTAMP(ts) * 1000 as ts,
    purchase_price,
    item_category as category,
    DATE_FORMAT(ts, 'yyyy-MM-dd') as ds
FROM parquet.`s3a://chronon/warehouse/data/purchases/purchases.parquet`
WHERE purchase_price > 0
ORDER BY ts
"""

# Create staging query that outputs to quickstart.purchases_from_minio_v1
v1 = StagingQuery(
    query=query,
    version=1,
    output_namespace="quickstart",
    dependencies=[],  # No dependencies since reading directly from S3
    table_properties={
        "provider": "iceberg",
        "write.format.default": "parquet"
    }
)

