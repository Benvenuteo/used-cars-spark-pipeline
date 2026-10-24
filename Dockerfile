FROM python:3.11.14-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless tini \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SPARK_LOCAL_IP=127.0.0.1 \
    OPENBLAS_NUM_THREADS=1 \
    OMP_NUM_THREADS=1 \
    MPLCONFIGDIR=/tmp/matplotlib

WORKDIR /app
COPY requirements.txt requirements-dev.txt requirements-notebooks.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt -r requirements-notebooks.txt
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir --no-deps . \
    && useradd --create-home --uid 1000 analyst \
    && mkdir -p /app/data /app/outputs \
    && chown -R analyst:analyst /app
COPY tests ./tests
COPY notebooks ./notebooks
USER analyst
ENTRYPOINT ["tini", "--"]
CMD ["cars", "--help"]
