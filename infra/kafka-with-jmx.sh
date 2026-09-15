#!/bin/sh
set -eu

export KAFKA_OPTS="-javaagent:/opt/kafka/jmx-exporter/jmx_prometheus_javaagent.jar=9404:/opt/kafka/jmx-exporter/config.yml ${KAFKA_OPTS:-}"
exec /__cacert_entrypoint.sh "$@"
