FROM python:3.12-slim as base

ENV PYTHONUNBUFFERED=1 \
    PYTHONOPTIMIZE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN curl -sSL https://install.python-poetry.org | python3 -

ENV PATH="/root/.local/bin:$PATH"

COPY pyproject.toml poetry.lock ./

RUN poetry config virtualenvs.create false && poetry install --no-root --no-dev

FROM base as run

COPY . .

EXPOSE 80

CMD ["gunicorn", "app.main:app", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:80", "--workers", "4", "--threads", "2"]

