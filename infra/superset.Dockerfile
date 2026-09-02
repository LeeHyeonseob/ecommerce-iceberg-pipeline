FROM apache/superset:4.1.3

USER root
# Athena 연결, PostgreSQL 메타DB, Redis 캐시에 필요한 드라이버
COPY superset/requirements.txt /tmp/superset-requirements.txt
RUN pip install --no-cache-dir -r /tmp/superset-requirements.txt
USER superset
