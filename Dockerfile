# Dockerfile
# Uses multi-stage builds requiring Docker 17.05 or higher
# See https://docs.docker.com/develop/develop-images/multistage-build/

# Creating a python base with shared environment variables
FROM pypy:3.9-slim as python-base
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv"

ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"
RUN apt-get update \
    && apt-get install --no-install-recommends -y libev4 libev-dev

# builder-base is used to build dependencies
FROM python-base as builder-base
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        curl \
        build-essential

# Install Poetry - respects $POETRY_VERSION & $POETRY_HOME
ENV POETRY_VERSION=1.3.2
RUN curl -sSL https://install.python-poetry.org | python3 -

# We copy our Python requirements here to cache them
# and install only runtime deps using poetry
WORKDIR $PYSETUP_PATH
COPY . ./

RUN poetry install --only main  # respects
# 'development' stage installs all dev deps and can be used to develop code.
# For example using docker-compose to mount local volume under /app
FROM python-base as development

# Copying poetry and venv into image
COPY --from=builder-base $POETRY_HOME $POETRY_HOME
COPY --from=builder-base $PYSETUP_PATH $PYSETUP_PATH

# venv already has runtime deps installed we get a quicker install
WORKDIR $PYSETUP_PATH
RUN poetry install
WORKDIR /
COPY . .
WORKDIR /gemini-python

# 'production' stage uses the clean 'python-base' stage and copyies
# in only our runtime deps that were installed in the 'builder-base'
FROM python-base as production

COPY --from=builder-base $VENV_PATH $VENV_PATH

COPY ./gemini_python /gemini-python/gemini_python
RUN ln -s /opt/pysetup/.venv/bin/gemini /gemini-python/gemini
ENV PYTHONPATH=/gemini-python
WORKDIR /gemini-python
