FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
# git: the pipeline is a pinned git dependency in pyproject.toml
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/* \
    && useradd -u 1000 -m app
WORKDIR /app
COPY pyproject.toml README.md ./
COPY geoecon_pipeline_mcp ./geoecon_pipeline_mcp
COPY pricing.yaml ./pricing.yaml
COPY fixtures ./fixtures
# The registry is data outside the pipeline package; ship the same file the CronJob image ships,
# at the path deploy/mcp-deployment.yaml points SOURCES_PATH at.
RUN mkdir -p /app/config && cp fixtures/sources.yaml /app/config/sources.yaml \
    && pip install --no-cache-dir . && mkdir -p /data/archive && chown -R app /app /data
USER 1000
EXPOSE 8000
ENTRYPOINT ["geoecon-pipeline-mcp"]
CMD ["--http", "--host", "0.0.0.0", "--port", "8000"]
