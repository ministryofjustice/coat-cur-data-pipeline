FROM ghcr.io/ministryofjustice/analytical-platform-airflow-python-base:1.22.0@sha256:1fbc48bbe820be43c5910af9990a47d537808470bb465beb6634e30f23f2aace

ARG MOJAP_IMAGE_VERSION="default"

ENV MOJAP_IMAGE_VERSION=${MOJAP_IMAGE_VERSION}

COPY requirements.txt requirements.txt
COPY scripts/ scripts/

RUN <<EOF
pip install --no-cache-dir --requirement requirements.txt
EOF

ENTRYPOINT ["python3", "scripts/main.py"]
