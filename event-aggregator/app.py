from prometheus_fastapi_instrumentator import Instrumentator
from fastapi import FastAPI
import os
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.propagate import extract
from opentelemetry.sdk.resources import Resource
import threading
import time
import redis
from kafka import KafkaConsumer
import json

app = FastAPI()
Instrumentator().instrument(app).expose(app)

resource = Resource.create({"service.name": "event-aggregator"})
trace.set_tracer_provider(TracerProvider(resource=resource))
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint=os.environ.get("TEMPO_ENDPOINT", "http://tempo:4318/v1/traces")))
)
FastAPIInstrumentor.instrument_app(app)

tracer = trace.get_tracer(__name__)

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)


def update_online_count(event_type):
    if event_type == "connect":
        redis_client.incr("online_count")
    elif event_type == "disconnect":
        redis_client.decr("online_count")


def consume_loop():
    consumer = KafkaConsumer(
        "player-events",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="aggregator-group"
    )
    print("Consumer 루프 시작, 메시지를 기다리는 중...")
    for message in consumer:
        carrier = {k: v.decode("utf-8") for k, v in (message.headers or [])}
        ctx = extract(carrier)

        with tracer.start_as_current_span("process_event", context=ctx):
            event = message.value
            event_type = event.get("event_type")
            time.sleep(0.2)  # 인위적으로 "무거운 처리"를 흉내냄 (Lag 재현용)
            update_online_count(event_type)
            print(f"처리함: {event}")


@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=consume_loop, daemon=True)
    thread.start()


@app.get("/health")
def health():
    return {"message": "OK"}


@app.get("/online-count")
def get_online_count():
    count = redis_client.get("online_count")
    return {"online_count": int(count) if count else 0}
