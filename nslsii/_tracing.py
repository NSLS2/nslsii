"""OpenTelemetry traces of Bluesky sessions for trace backends."""


def configure_bluesky_tracing() -> None:
    """
    Export the OpenTelemetry spans of this process, such as those of bluesky's
    RunEngine runs and messages and of ophyd's Status objects, to an OTLP/HTTP
    endpoint.

    It is configured entirely by the standard OpenTelemetry environment
    variables, for example:

    - ``OTEL_SERVICE_NAME`` and ``OTEL_RESOURCE_ATTRIBUTES``: attributes of
      every span, such as the beamline; the service name defaults to
      ``bluesky``
    - ``OTEL_EXPORTER_OTLP_ENDPOINT``: the collector (default
      ``http://localhost:4318``)
    - ``OTEL_TRACES_SAMPLER``: which traces to keep (default all)
    - ``OTEL_BSP_SCHEDULE_DELAY``: milliseconds between exports (default 5000)
    - ``OTEL_EXPORTER_OTLP_TIMEOUT``: seconds to wait for the collector, which
      also delays exit when it is unreachable (default 10)
    - ``OTEL_SDK_DISABLED=true``: record and export nothing

    Spans are exported in batches by a background thread, and the rest when
    the interpreter exits. A ``multiprocessing`` child started by fork, such
    as the Queue Server worker, exports the rest only on Python 3.13 and
    later; on earlier versions they are lost when it exits.
    """
    # Imported here so that ``import nslsii`` does not pay for the SDK when tracing is not used.
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import SERVICE_NAME, OTELResourceDetector, Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    resource = Resource.create()
    if SERVICE_NAME not in OTELResourceDetector().detect().attributes:
        resource = resource.merge(Resource({SERVICE_NAME: "bluesky"}))

    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
