from fastapi import FastAPI
from kafka import KafkaProducer
from prometheus_fastapi_instrumentator import Instrumentator
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.propagate import inject
from opentelemetry.sdk.resources import Resource
import os
import json

app = FastAPI()
Instrumentator().instrument(app).expose(app)

resource = Resource.create({"service.name": "event-ingest"})
trace.set_tracer_provider(TracerProvider(resource=resource))
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint=os.environ.get("TEMPO_ENDPOINT", "http://tempo:4318/v1/traces")))
)
FastAPIInstrumentor.instrument_app(app)

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def on_success(metadata):
    print(f"전송 성공: {metadata.topic} 파티션 {metadata.partition}")


def on_error(exc):
    print(f"전송 실패! 이유: {exc}")


@app.get("/health")
def health():
    return {"message": "OK"}


@app.post("/events")
def create_event(event: dict):
    headers_carrier = {}
    inject(headers_carrier)
    kafka_headers = [(k, v.encode("utf-8")) for k, v in headers_carrier.items()]

    future = producer.send("player-events", value=event, headers=kafka_headers)
    future.add_callback(on_success)
    future.add_errback(on_error)
    return {"status": "발행 시도됨", "data": event}
