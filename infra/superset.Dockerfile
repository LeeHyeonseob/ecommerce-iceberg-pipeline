FROM apache/superset:4.1.3

USER root
# Athena 연결, PostgreSQL 메타DB, Redis 캐시에 필요한 드라이버
RUN pip install --no-cache-dir \
    "PyAthena[SQLAlchemy]>=3.0,<4.0" \
    "psycopg2-binary>=2.9,<3.0" \
    "redis>=4.6,<5.0"
USER superset
