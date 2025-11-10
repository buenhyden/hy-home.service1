import json
import os
from contextlib import contextmanager

import psycopg2
import redis
from fastapi import Depends, FastAPI, Request
from kafka import KafkaProducer

app = FastAPI()

# --- 환경 변수에서 연결 정보 로드 (Secret을 통해 주입됨) ---
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST", "postgresql.db.svc.cluster.local")
DB_NAME = os.environ.get("DB_NAME")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis-master.redis.svc.cluster.local")
REDIS_PASS = os.environ.get("REDIS_PASSWORD")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka.kafka.svc.cluster.local:9092")


# --- 서비스 연결 (간단한 예시) ---
# (실제로는 커넥션 풀링, 재시도 로직 등이 필요합니다)
@contextmanager
def get_db_connection():
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )
    try:
        yield conn
    finally:
        conn.close()


def get_redis_client():
    return redis.Redis(
        host=REDIS_HOST, password=REDIS_PASS, port=6379, db=0, decode_responses=True
    )


def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


@app.get("/")
async def root():
    return {"message": f"Welcome to hy-home.local SaaS! (DB_USER: {DB_USER})"}


@app.post("/track")
async def track_event(
    request: Request,
    r: redis.Redis = Depends(get_redis_client),
    producer: KafkaProducer = Depends(get_kafka_producer),
):
    event_data = await request.json()

    # 1. Kafka로 이벤트 즉시 전송
    producer.send("logs", event_data)

    # 2. Redis에 실시간 집계 (예: 이벤트 타입 카운트)
    event_type = event_data.get("type", "unknown")
    r.incr(f"events_count:{event_type}")

    return {"status": "event received"}


@app.get("/stats")
async def get_stats(r: redis.Redis = Depends(get_redis_client)):
    # 3. Redis에서 집계 결과 조회
    event_type = "page_view"  # 예시
    count = r.get(f"events_count:{event_type}")
    return {"event_type": event_type, "count": count or 0}


# (4. 별도의 Consumer가 Kafka 'logs' 토픽을 구독하여
#    PostgreSQL과 Elasticsearch에 저장하는 로직은
#    이 API 서버와 다른 별도의 Deployment로 배포되어야 합니다.)
