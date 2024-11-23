## For local testing:
Starting db:
```shell
docker compose up db
```

Start the fast api
```shell
cd backend
poetry config virtualenvs.create false && poetry install --no-root --no-dev
poetry run fastapi dev main.py
```

## For Docker
```shell
docker compose up --build
```