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

from gen_thrift.api.ttypes import StagingQuery, MetaData

# SQL query to transform the data from the external table
query = """
SELECT
    user_id,
    email,
    email_verified,
    account_created_ts,
    age,
    country,
    ds
FROM quickstart_users_external
WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    AND age >= 18
ORDER BY ds, user_id
"""

# Staging query that loads Parquet from MinIO and creates an Iceberg table
v1 = StagingQuery(
    metaData=MetaData(
        name="users_from_minio",
        outputNamespace="quickstart",
        tableProperties={
            "description": "Users data loaded from MinIO and transformed to Iceberg"
        }
    ),
    query=query,
    startPartition="2023-11-01",
    setups=[
        # Create external table pointing to MinIO S3 bucket with Parquet data
        """
        CREATE EXTERNAL TABLE IF NOT EXISTS quickstart_users_external (
            user_id STRING,
            email STRING,
            email_verified BOOLEAN,
            account_created_ts BIGINT,
            age INT,
            country STRING,
            ds STRING
        )
        STORED AS PARQUET
        LOCATION 's3a://chronon/warehouse/data/users/'
        """
    ]
)

