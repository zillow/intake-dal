ARG TAG=2.1.6b34e4f3

FROM ai-docker.artifactory.zgtools.net/artificial-intelligence/ai-platform/aip-infrastructure/dockerfiles/python-debian/python-debian:2.1.6b34e4f3

ENV PYTHONPATH="/opt/zillow:${PYTHONPATH}"

RUN apt-get update && apt-get install -y \
    g++ libyaml-dev git-core libjpeg-dev bash vim jq

COPY .flake8 README.rst pyproject.toml poetry.lock ./

COPY intake_dal ./intake_dal

RUN pip install --upgrade pip && pip install --upgrade poetry

RUN poetry install --no-interaction -v
