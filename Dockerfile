FROM ghcr.io/ministryofjustice/analytical-platform-airflow-python-base:1.23.0@sha256:7455d1712e9671b8dabc8a37e5576ca890509ebdfdc2399a0baf6dd2d1dcaa96

ARG MOJAP_IMAGE_VERSION="default"

ENV MOJAP_IMAGE_VERSION=${MOJAP_IMAGE_VERSION}

COPY requirements.txt requirements.txt
COPY scripts/ scripts/

RUN <<EOF
pip install --no-cache-dir --requirement requirements.txt
EOF

ENTRYPOINT ["python3", "scripts/main.py"]
