FROM python:3.12-slim as base

ENV PYTHONUNBUFFERED=1 \
    PYTHONOPTIMIZE=1

WORKDIR /app

RUN apt-get update
RUN apt-get -y install curl
RUN curl -sSL https://install.python-poetry.org | python3 -

ENV PATH="/root/.local/bin:$PATH"

COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false && poetry install --no-root --no-dev


FROM base as run
COPY . .

EXPOSE 8000
# gunicorn main:app -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:80 --workers 4 --threads 2
CMD ["gunicorn", "main:app", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000", "--workers", "4", "--threads", "2"]

