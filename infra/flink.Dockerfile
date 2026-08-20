FROM flink:1.19-scala_2.12

# S3 쓰기 플러그인 활성화 (plugins/로 복사해야 인식됨)
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev wget default-jdk && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# pemja(PyFlink 의존성) 소스빌드에 JDK 필요 (배경: CLAUDE.md §13-1)
ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN pip3 install apache-flink==1.19.3

# Kafka 커넥터
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar

# Parquet 포맷 어댑터
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-parquet/1.19.3/flink-parquet-1.19.3.jar

# Hadoop Configuration 등 (flink-s3-fs-hadoop plugins/는 격리된 클래스로더라 재사용 불가, 배경: CLAUDE.md §13-1)
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar && \
    wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar

# Parquet 구현체. 개별 parquet-column jar에 codegen 클래스 누락 이슈가 있어 bundle 사용 (배경: CLAUDE.md §15-1)
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/parquet/parquet-hadoop-bundle/1.13.1/parquet-hadoop-bundle-1.13.1.jar
