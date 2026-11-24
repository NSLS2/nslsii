"""OpenTelemetry traces of Bluesky sessions for trace backends."""


def configure_bluesky_tracing() -> None:
    """
    Export the OpenTelemetry spans of this process, such as those of bluesky's
    RunEngine runs and messages and of ophyd's Status objects, to an OTLP/HTTP
    endpoint.

    It is configured entirely by the standard OpenTelemetry environment
    variables, for example:

    - ``OTEL_SERVICE_NAME`` and ``OTEL_RESOURCE_ATTRIBUTES``: attributes of
      every span, such as the beamline
    - ``OTEL_EXPORTER_OTLP_ENDPOINT``: the collector (default
      ``http://localhost:4318``)
    - ``OTEL_TRACES_SAMPLER``: which traces to keep (default all)
    - ``OTEL_BSP_SCHEDULE_DELAY``: milliseconds between exports (default 5000)
    - ``OTEL_SDK_DISABLED=true``: record and export nothing

    Spans are exported in batches by a background thread. A process that ends
    with ``os._exit``, as a forked ``multiprocessing`` child such as the Queue
    Server worker does, loses the spans not yet exported.

    The RunEngine spans of a plan are in separate traces unless the plan is
    wrapped with ``bluesky.tracing.trace_plan``, which makes them one trace.
    """
    # Imported here so that ``import nslsii`` does not pay for the SDK when tracing is not used.
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(resource=Resource.create())
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
