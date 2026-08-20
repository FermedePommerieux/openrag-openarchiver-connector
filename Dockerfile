FROM python:3.13-slim

ARG SOURCE_SHA="unknown"

LABEL org.opencontainers.image.title="OpenRAG OpenArchiver Connector" \
      org.opencontainers.image.description="Ingest OpenArchiver mail archives into OpenRAG" \
      org.opencontainers.image.source="https://github.com/FermedePommerieux/openrag-openarchiver-connector" \
      org.opencontainers.image.revision="${SOURCE_SHA}"

RUN groupadd --gid 1000 connector \
    && useradd --uid 1000 --gid 1000 --no-create-home --home-dir /app connector

WORKDIR /app
COPY --chown=connector:connector connector.py /app/connector.py

USER 1000:1000
EXPOSE 8080

ENTRYPOINT ["python", "/app/connector.py"]
