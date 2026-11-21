#!/bin/sh
# Runs in the es-init container: ingest pipeline (adds ingested_at) + index template for both result indices.
set -e
ES=http://elasticsearch:9200
H='Content-Type: application/json'
until curl -sf "$ES/_cluster/health?wait_for_status=yellow&timeout=5s" > /dev/null; do echo waiting for ES; sleep 2; done

curl -sf -X PUT "$ES/_ingest/pipeline/add-ingested-at" -H "$H" -d '{
  "description": "stamp arrival time in Elasticsearch (used to compute delivery latency)",
  "processors": [ { "set": { "field": "ingested_at", "value": "{{{_ingest.timestamp}}}" } } ]
}'
echo

curl -sf -X PUT "$ES/_index_template/demo-logs" -H "$H" -d '{
  "index_patterns": ["otel-logs", "kafka-logs", "kafka-logs-replay"],
  "priority": 500,
  "template": {
    "settings": { "number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "1s",
                  "index.default_pipeline": "add-ingested-at" }
  }
}'
echo
echo "es-init done"
