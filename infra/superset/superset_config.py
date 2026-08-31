import os
from urllib.parse import quote_plus

# boto3는 AWS_DEFAULT_REGION을 본다. .env에는 AWS_REGION으로 들어있다
os.environ.setdefault("AWS_DEFAULT_REGION", os.environ.get("AWS_REGION", "ap-northeast-2"))

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

_DB_USER = quote_plus(os.environ["POSTGRES_USER"])
_DB_PASSWORD = quote_plus(os.environ["POSTGRES_PASSWORD"])
_DB_HOST = os.environ.get("POSTGRES_HOST", "postgres")
_DB_PORT = os.environ.get("POSTGRES_PORT", "5432")
_DB_NAME = quote_plus(os.environ["POSTGRES_DB"])

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{_DB_USER}:{_DB_PASSWORD}"
    f"@{_DB_HOST}:{_DB_PORT}/{_DB_NAME}"
)

# Athena는 쿼리당 1~3초 + 스캔량 과금이라 캐시가 없으면 차트 수만큼 왕복한다.
# TTL만으로는 배치 직후가 어긋난다 - Airflow DAG 마지막에 캐시를 무효화해야 한다
_CACHE = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_REDIS_URL": os.environ.get("REDIS_URL", "redis://redis:6379/0"),
    "CACHE_DEFAULT_TIMEOUT": 86400,
}
RATELIMIT_STORAGE_URI = os.environ.get("REDIS_URL", "redis://redis:6379/0")
CACHE_CONFIG = {**_CACHE, "CACHE_KEY_PREFIX": "superset_metadata_"}
DATA_CACHE_CONFIG = {**_CACHE, "CACHE_KEY_PREFIX": "superset_data_"}
FILTER_STATE_CACHE_CONFIG = {**_CACHE, "CACHE_KEY_PREFIX": "superset_filter_"}
EXPLORE_FORM_DATA_CACHE_CONFIG = {**_CACHE, "CACHE_KEY_PREFIX": "superset_form_"}

SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300

FEATURE_FLAGS = {"DASHBOARD_CROSS_FILTERS": True}
