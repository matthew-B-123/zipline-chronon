# Chronon Bootcamp: Local Feature Engineering Pipeline

This guide walks through setting up a complete Chronon feature engineering pipeline locally using Docker, MinIO (S3), Spark, and Iceberg.

## 🎯 What We Built

A production-ready offline feature engineering workflow:

```
Raw Data (MinIO S3)
    ↓
StagingQuery (ETL)
    ↓
Iceberg Table
    ↓
GroupBy (Aggregations)
    ↓
Feature Table (Iceberg) → [Optional] DynamoDB (Online Serving)
```

### Key Components
- **MinIO**: S3-compatible object storage for raw data and Iceberg tables
- **Spark 3.5.3**: Distributed computation engine
- **Iceberg 1.10.0**: Table format with ACID transactions and time travel
- **MongoDB**: Chronon metadata storage
- **DynamoDB Local**: Online feature serving (KV store)
- **Chronon**: Feature engineering platform

---

## 🏗️ Architecture

### Services
- **MinIO** (ports 9000, 9001): S3-compatible storage
- **MongoDB** (port 27017): Metadata storage
- **DynamoDB** (port 8000): Online feature serving
- **Spark Master** (ports 7077, 8080): Cluster coordinator
- **Spark Workers** (2 replicas): Compute workers
- **Chronon** (port 4040): Python environment for compiling configs

### Storage Layout
```
s3a://chronon/warehouse/
├── data/
│   └── purchases/
│       └── purchases.parquet          # Raw data
├── quickstart/
│   ├── quickstart_purchases_from_minio_v1__1/  # Staging query output (Iceberg)
│   └── quickstart_purchase_features_v1__1/     # GroupBy output (Iceberg)
```

---

## 🚀 Setup Instructions

### Prerequisites
- Docker & Docker Compose
- Mill build tool (for building Chronon JAR)
- Python 3.11+ (for local compilation)

### 1. Build Chronon JAR

```bash
# Build the Chronon Spark assembly with all dependencies
mill spark/assembly

# The output will be at: out/spark/assembly.dest/out.jar
```

This JAR contains:
- Chronon Spark runtime
- Spark 3.5.3 dependencies
- Iceberg 1.10.0 runtime
- Hadoop AWS libraries

### 2. Generate Sample Data

Create sample purchase data:

```bash
# Generate purchases.parquet and users.parquet in sample_data/
python3 scripts/generate_sample_parquet.py
```

Sample data specs:
- **155 purchase records** spanning Dec 1-7, 2023
- **100 user records**
- Categories: electronics, books, clothing, food, home

### 3. Start Docker Services

```bash
# Start all services
docker-compose up -d

# Verify all containers are healthy
docker-compose ps
```

Services should show:
- ✅ minio (healthy)
- ✅ mongodb (healthy)
- ✅ dynamodb (healthy)
- ✅ spark-master (running)
- ✅ spark-worker (2 replicas)
- ✅ chronon (running)

### 4. Upload Sample Data to MinIO

```bash
# Configure MinIO client
docker run --rm --network zipline-chronon_default \
  --entrypoint=/bin/sh minio/mc -c "
  mc alias set myminio http://minio:9000 minioadmin minioadmin
  mc mb -p myminio/chronon/warehouse/data/purchases
  mc cp /data/purchases.parquet myminio/chronon/warehouse/data/purchases/
" -v $(pwd)/sample_data:/data

# Verify upload
docker run --rm --network zipline-chronon_default \
  --entrypoint=/bin/sh minio/mc -c "
  mc alias set myminio http://minio:9000 minioadmin minioadmin
  mc ls myminio/chronon/warehouse/data/purchases/
"
```

Or access MinIO UI at http://localhost:9001 (minioadmin/minioadmin)

### 5. Create Team Configuration

The `teams.json` file defines namespaces and table properties:

```json
{
  "default": {
    "namespace": "default",
    "table_properties": {}
  },
  "quickstart": {
    "namespace": "quickstart",
    "table_properties": {}
  }
}
```

This file is already at `python/test/sample/teams.json`.

---

## 📝 Workflow 1: Staging Query (ETL)

### What is a Staging Query?
A StagingQuery is Chronon's ETL primitive that:
- Reads raw data from any source (S3, databases, etc.)
- Transforms it with SQL
- Writes to a structured table (Iceberg, Hive, etc.)

### Code Example

See `python/test/sample/staging_queries/quickstart/purchases_from_minio.py`:

```python
from ai.chronon.staging_query import StagingQuery

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

v1 = StagingQuery(
    query=query,
    version=1,
    output_namespace="quickstart",
    dependencies=[],
    table_properties={
        "provider": "iceberg",
        "write.format.default": "parquet"
    }
)
```

### Compile and Run

```bash
# 1. Compile the staging query to JSON
docker-compose exec chronon bash -c "
  cd /srv/chronon && \
  python3 /srv/chronon-python/src/ai/chronon/repo/compile.py \
    --chronon-root /srv/chronon \
    --ignore-python-errors
"

# Output: compiled/staging_queries/quickstart/purchases_from_minio.v1__1

# 2. Copy compiled JSON to Spark container
docker cp python/test/sample/compiled/staging_queries/quickstart/purchases_from_minio.v1__1 \
  zipline-chronon-spark-master-1:/tmp/

# 3. Run with spark-submit
docker-compose exec -u root spark-master bash -c "
/opt/spark/bin/spark-submit \
  --master local[*] \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hadoop \
  --conf spark.sql.catalog.spark_catalog.warehouse=s3a://chronon/warehouse \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
  --class ai.chronon.spark.Driver \
  /tmp/chronon-spark.jar \
  staging-query-backfill \
  --conf-path=/tmp/purchases_from_minio.v1__1 \
  --end-date=2023-12-07
"
```

### Verify Output

```bash
docker-compose exec -u root spark-master bash -c "
/opt/spark/bin/spark-sql \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hadoop \
  --conf spark.sql.catalog.spark_catalog.warehouse=s3a://chronon/warehouse \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
  -e 'SELECT COUNT(*) FROM quickstart.quickstart_purchases_from_minio_v1__1;'
"
# Expected: 155 rows
```

---

## 🔧 Workflow 2: GroupBy (Feature Aggregations)

### What is a GroupBy?
A GroupBy computes aggregated features over time windows:
- Rolling sums, counts, averages, max/min
- Multiple time windows (1d, 7d, 30d)
- Outputs to offline tables AND online KV stores

### Code Example

See `python/test/sample/group_bys/quickstart/purchase_features.py`:

```python
from gen_thrift.api.ttypes import EventSource, Source
from ai.chronon.group_by import Aggregation, GroupBy, Operation
from ai.chronon.query import Query, selects

# Read from Iceberg table created by staging query
source = Source(
    events=EventSource(
        table="quickstart.quickstart_purchases_from_minio_v1__1",
        query=Query(
            selects=selects("user_id", "purchase_price", "category"),
            time_column="ts"
        )
    )
)

windows = ["1d", "7d", "30d"]

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    online=True,
    version=1,
    backfill_start_date="2023-12-01",
    aggregations=[
        Aggregation(
            input_column="purchase_price",
            operation=Operation.SUM,
            windows=windows
        ),
        Aggregation(
            input_column="purchase_price",
            operation=Operation.COUNT,
            windows=windows
        ),
        Aggregation(
            input_column="purchase_price",
            operation=Operation.AVERAGE,
            windows=windows
        ),
        Aggregation(
            input_column="purchase_price",
            operation=Operation.MAX,
            windows=windows
        ),
    ],
)
```

### Compile and Run

```bash
# 1. Compile the GroupBy
docker-compose exec chronon bash -c "
  cd /srv/chronon && \
  python3 /srv/chronon-python/src/ai/chronon/repo/compile.py \
    --chronon-root /srv/chronon \
    --ignore-python-errors
"

# 2. Copy compiled JSON
docker cp python/test/sample/compiled/group_bys/quickstart/purchase_features.v1__1 \
  zipline-chronon-spark-master-1:/tmp/

# 3. Run backfill
docker-compose exec -u root spark-master bash -c "
/opt/spark/bin/spark-submit \
  --master local[*] \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hadoop \
  --conf spark.sql.catalog.spark_catalog.warehouse=s3a://chronon/warehouse \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
  --class ai.chronon.spark.Driver \
  /tmp/chronon-spark.jar \
  group-by-backfill \
  --conf-path=/tmp/purchase_features.v1__1 \
  --end-date=2023-12-07
"
```

### Verify Features

```bash
docker-compose exec -u root spark-master bash -c "
/opt/spark/bin/spark-sql \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hadoop \
  --conf spark.sql.catalog.spark_catalog.warehouse=s3a://chronon/warehouse \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
  -e 'SELECT * FROM quickstart.quickstart_purchase_features_v1__1 LIMIT 5;'
"
```

Expected output columns:
- `user_id`
- `purchase_price_sum_1d`, `purchase_price_sum_7d`, `purchase_price_sum_30d`
- `purchase_price_count_1d`, `purchase_price_count_7d`, `purchase_price_count_30d`
- `purchase_price_average_1d`, `purchase_price_average_7d`, `purchase_price_average_30d`
- `purchase_price_max_1d`, `purchase_price_max_7d`, `purchase_price_max_30d`

---

## 🎓 Key Concepts

### 1. Compilation vs Execution
- **Compilation** (Python → JSON): Happens locally or in `chronon` container
  - Python API converts to Thrift JSON
  - No Spark cluster needed
  - Fast (<5 seconds)
  
- **Execution** (Spark job): Happens on Spark cluster
  - Reads compiled JSON
  - Executes distributed computation
  - Writes to Iceberg tables

### 2. Iceberg Table Properties
To create Iceberg tables (not Hive text tables), specify:
```python
table_properties={
    "provider": "iceberg",
    "write.format.default": "parquet"
}
```

### 3. Spark Configurations
Key configs for S3 + Iceberg:
```bash
# S3 (MinIO) access
--conf spark.hadoop.fs.s3a.access.key=minioadmin
--conf spark.hadoop.fs.s3a.secret.key=minioadmin
--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000
--conf spark.hadoop.fs.s3a.path.style.access=true
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem

# Iceberg catalog
--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.spark_catalog.type=hadoop
--conf spark.sql.catalog.spark_catalog.warehouse=s3a://chronon/warehouse

# Required packages
--packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0
```

### 4. Hadoop Catalog
We use Iceberg's Hadoop catalog (not Hive Metastore):
- Metadata stored in warehouse directory alongside data
- No external metastore needed
- Simpler for local development
- **Limitation**: Cannot use custom table locations with `CREATE EXTERNAL TABLE`

---

## 🔄 Production Deployment

### Changing Storage Backend
To point at your company's infrastructure, only update Spark configs:

**AWS S3 + Glue Catalog:**
```bash
--conf spark.sql.catalog.spark_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
--conf spark.sql.catalog.spark_catalog.warehouse=s3://your-company-bucket/iceberg/
--conf spark.sql.catalog.spark_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

**Azure ADLS + Unity Catalog:**
```bash
--conf spark.sql.catalog.spark_catalog.catalog-impl=org.apache.iceberg.unity.UnityCatalog
--conf spark.sql.catalog.spark_catalog.warehouse=abfss://container@account.dfs.core.windows.net/iceberg/
```

**GCS + Polaris:**
```bash
--conf spark.sql.catalog.spark_catalog.warehouse=gs://your-company-bucket/iceberg/
--conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
```

**The Python code (staging queries, group bys) stays the same!**

### Online Serving with DynamoDB
To upload features to DynamoDB for low-latency serving:

```bash
# Configure KV store in teams.json
{
  "quickstart": {
    "namespace": "quickstart",
    "online_data_provider": {
      "type": "dynamodb",
      "region": "us-west-2",
      "table": "chronon_features"
    }
  }
}

# Upload features
spark-submit \
  --class ai.chronon.spark.Driver \
  chronon-spark.jar \
  group-by-streaming-upload \
  --conf-path=purchase_features.v1__1
```

---

## 🐛 Troubleshooting

### Issue: "MetaException: Version information not found"
**Cause**: Hive metastore warnings (can be ignored)  
**Solution**: These are warnings, not errors. Job still succeeds.

### Issue: "NoClassDefFoundError: ResolvedIdentifier"
**Cause**: Spark version mismatch (Iceberg 1.5 needs Spark 3.5+)  
**Solution**: Ensure using Spark 3.5.3 and `iceberg-spark-runtime-3.5_2.12:1.5.0`

### Issue: "NullPointerException in SparkContext"
**Cause**: Missing S3/Iceberg configurations  
**Solution**: Ensure all `--conf` flags are passed to spark-submit

### Issue: Table is Hive text format instead of Iceberg
**Cause**: Missing `table_properties` in StagingQuery  
**Solution**: Add `table_properties={"provider": "iceberg"}` to your StagingQuery

### Issue: "PARSE_SYNTAX_ERROR: missing 'AND'"
**Cause**: Template rendering bug with date literals  
**Solution**: Remove date filters or use subqueries to avoid template variables

---

## 📚 Next Steps

1. **Create Joins**: Combine multiple GroupBys into training sets
2. **Add Labels**: Define label sources for ML training
3. **Streaming**: Process real-time events with Flink
4. **Model Serving**: Integrate with ML serving infrastructure
5. **Monitoring**: Add observability with OpenTelemetry

---

## 📁 Project Structure

```
zipline-chronon/
├── docker-compose.yml                    # Service definitions
├── out/spark/assembly.dest/out.jar       # Chronon Spark JAR (built by Mill)
├── sample_data/
│   ├── purchases.parquet                 # Raw purchase data
│   └── users.parquet                     # Raw user data
├── python/
│   ├── src/ai/chronon/                   # Chronon Python API
│   │   ├── repo/compile.py               # Compilation script
│   │   └── repo/run.py                   # Execution script
│   └── test/sample/
│       ├── teams.json                    # Team/namespace config
│       ├── staging_queries/
│       │   └── quickstart/
│       │       └── purchases_from_minio.py
│       ├── group_bys/
│       │   └── quickstart/
│       │       └── purchase_features.py
│       └── compiled/                     # Compiled JSON configs
│           ├── staging_queries/
│           └── group_bys/
└── scripts/
    └── generate_sample_parquet.py        # Data generation script
```

---

## 🙌 Credits

Built with:
- [Chronon](https://github.com/airbnb/chronon) - Feature engineering platform
- [Apache Spark](https://spark.apache.org/) - Distributed computing
- [Apache Iceberg](https://iceberg.apache.org/) - Table format
- [MinIO](https://min.io/) - S3-compatible storage
- [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) - KV store

---

## 📄 License

See LICENSE.txt for details.

