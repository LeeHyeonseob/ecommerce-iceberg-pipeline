FROM apache/superset:4.1.3

USER root
# Athena용 SQLAlchemy 드라이버
RUN pip install --no-cache-dir "PyAthena[SQLAlchemy]>=3.0,<4.0"
USER superset
