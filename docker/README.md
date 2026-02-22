# Infrastructure Guide

## Services Overview

| Service | Container | Port(s) | Purpose |
|---------|-----------|---------|---------|
| Zookeeper | `sma-zookeeper` | 2181 | Kafka coordination |
| Kafka | `sma-kafka` | 9092 (internal), 29092 (host) | Event streaming |
| Kafka UI | `sma-kafka-ui` | 8082 | Kafka monitoring dashboard |
| MinIO | `sma-minio` | 9000 (API), 9001 (console) | S3-compatible data lake |
| Spark Master | `sma-spark-master` | 8080 (UI), 7077 (submit) | Spark cluster manager |
| Spark Worker 1 | `sma-spark-worker-1` | — | Spark executor |
| Spark Worker 2 | `sma-spark-worker-2` | — | Spark executor |
| PostgreSQL | `sma-postgres` | 5432 | Airflow metadata DB |
| Redis | `sma-redis` | 6379 | Airflow task queue |
| Airflow Webserver | `sma-airflow-webserver` | 8090 | Airflow UI + API |
| Airflow Scheduler | `sma-airflow-scheduler` | — | DAG scheduler |
| Airflow Worker | `sma-airflow-worker` | — | Task executor |

## Startup Order (handled automatically by healthchecks)

```
zookeeper → kafka → kafka-init (topics)
minio → minio-init (buckets)
postgres + redis → airflow-init → airflow-webserver + airflow-scheduler + airflow-worker
spark-master → spark-worker-1 + spark-worker-2
```

## Service URLs

- **Airflow UI**: http://localhost:8090 — `admin / admin`
- **MinIO Console**: http://localhost:9001 — `minioadmin / minioadmin`
- **Spark Master UI**: http://localhost:8080
- **Kafka UI**: http://localhost:8082

## Data Lake Buckets (MinIO)

| Bucket | Purpose | Path Pattern |
|--------|---------|--------------|
| `bronze` | Raw ingested data | `bronze/{source}/year=YYYY/month=MM/day=DD/` |
| `silver` | Cleaned Parquet data | `silver/{entity}/year=YYYY/month=MM/day=DD/` |
| `gold` | Analytics-ready tables | `gold/{table_name}/` |
| `landing-zone` | Batch file drop area | `landing-zone/{source}/{filename}` |
| `quality-reports` | GE HTML data docs | `quality-reports/{checkpoint}/{run_id}/` |

## Kafka Topics

| Topic | Partitions | Retention | Producer |
|-------|-----------|-----------|----------|
| `social.tweets` | 3 | 7 days | Twitter Simulator |
| `social.engagements` | 3 | 7 days | Engagement Simulator |
| `social.youtube` | 2 | 7 days | YouTube Simulator |
| `social.reddit` | 2 | 7 days | Reddit Simulator |
| `social.events.dlq` | 1 | 30 days | Error handler |

## Common Operations

```bash
# View Kafka topics
docker exec sma-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Consume messages from tweets topic (debug)
docker exec sma-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic social.tweets \
  --from-beginning \
  --max-messages 10

# List MinIO buckets
docker exec sma-minio mc ls local/

# Check Airflow DAG status
docker exec sma-airflow-scheduler airflow dags list

# Submit Spark job manually
docker exec sma-spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/src/transformation/spark_jobs/bronze_to_silver.py
```

## Troubleshooting

**Kafka won't start**: Check Zookeeper is healthy first.
```bash
docker logs sma-zookeeper
docker logs sma-kafka
```

**Airflow tasks fail with "No module named X"**: Rebuild the Airflow image.
```bash
docker compose -f docker/docker-compose.yml build airflow-webserver
```

**MinIO not accessible**: Check port 9000 isn't in use.
```bash
lsof -i :9000
```

**Spark job fails with S3A error**: Verify MinIO is running and buckets exist.
```bash
docker exec sma-minio mc ls local/
```
