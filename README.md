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

## For GUI
go to [GUI repo](https://github.com/Sleepless-Samurais/check24-challenge-GUI/tree/main) and follow the readme for more information
