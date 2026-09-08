#!/usr/bin/env bash
# List every Kafka topic, then highlight the CDC data topics.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

echo "=== all topics ==="
kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

echo
echo "=== CDC data topics (prefix '${TOPIC_PREFIX}.') ==="
kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list \
  | grep -E "^${TOPIC_PREFIX}\." || echo "(none yet - register the connector and generate a change)"

echo
echo "=== describe CDC data topics ==="
for t in "${TOPIC_PREFIX}.public.users" "${TOPIC_PREFIX}.public.sales"; do
  kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --describe --topic "${t}" 2>/dev/null || echo "${t}: not created yet"
done
