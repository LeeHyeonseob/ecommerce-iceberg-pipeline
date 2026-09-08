# 002. S3 scheme과 Flink classpath 호환 실패

## 시도한 날짜

날짜 미기록

## 시도한 이유

Spark Iceberg 유지보수와 Flink Parquet S3 적재를 기본 런타임 의존성만으로 실행하려 했다.

## 실패 증상

- Iceberg `remove_orphan_files`에서 `UnsupportedFileSystemException: No FileSystem for scheme "s3"`가 발생했다.
- Flink S3 plugin의 격리된 classloader 때문에 Parquet job이 필요한 Hadoop class를 재사용하지 못했다.
- 개별 Parquet jar 조합에서는 필요한 codegen class가 누락됐다.

## 실패 원인

- Hadoop 3.x에는 `s3` scheme의 기본 구현 매핑이 없다.
- Flink plugin 디렉터리와 job classpath는 격리된다.
- 개별 Parquet 모듈만 추가하면 PyFlink 실행에 필요한 구현 클래스가 모두 포함되지 않는다.

## 현재 대안

- Spark에서 `fs.s3.impl`을 S3A 구현으로 명시하고 region을 설정한다.
- Spark 3.5와 맞는 `hadoop-aws` 3.3.4를 사용한다.
- Flink job classpath에 Hadoop client API/runtime을 별도로 둔다.
- Parquet 구현은 `parquet-hadoop-bundle`을 사용한다.

## 에이전트 지침

Spark, Hadoop, Iceberg, Flink jar 버전을 독립적으로 올리지 않는다. 버전 변경 시 S3 읽기, Parquet 쓰기와 `remove_orphan_files`까지 함께 검증한다.
