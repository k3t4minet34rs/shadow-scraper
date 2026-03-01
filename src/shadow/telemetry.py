# telemetry.py
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes


SERVICE_NAME = "shadow-py"


def init_metrics_otlp(endpoint: str) -> None:
    """
    endpoint example: http://localhost:8428/opentelemetry
    """
    resource = Resource.create({ResourceAttributes.SERVICE_NAME: SERVICE_NAME})

    exporter = OTLPMetricExporter(
        endpoint=f"{endpoint}/v1/metrics",
        # compression="gzip", headers={...} if you want
    )

    reader = PeriodicExportingMetricReader(exporter)
    provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(provider)


def get_meter():
    return metrics.get_meter(SERVICE_NAME)
