FROM apache/kafka:4.3.1

ARG JMX_EXPORTER_VERSION=1.6.0

RUN mkdir -p /opt/kafka/jmx-exporter \
    && wget -q -O /opt/kafka/jmx-exporter/jmx_prometheus_javaagent.jar \
      "https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar"

COPY kafka-jmx.yml /opt/kafka/jmx-exporter/config.yml
COPY kafka-with-jmx.sh /opt/kafka/jmx-exporter/run.sh

ENTRYPOINT ["/bin/sh", "/opt/kafka/jmx-exporter/run.sh"]
CMD ["/etc/kafka/docker/run"]
