FROM ghcr.io/ministryofjustice/analytical-platform-airflow-python-base:1.24.0@sha256:cf7754468c74520ac27aa78309db52dccae3ac5ac88e05d7771e3b2a738ed895

ARG MOJAP_IMAGE_VERSION="default"

ENV MOJAP_IMAGE_VERSION=${MOJAP_IMAGE_VERSION}

COPY requirements.txt requirements.txt
COPY scripts/ scripts/

RUN <<EOF
pip install --no-cache-dir --requirement requirements.txt
EOF

ENTRYPOINT ["python3", "scripts/main.py"]
