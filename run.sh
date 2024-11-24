docker stop $(docker ps -a -q)
docker rm $(docker ps -a -q)
docker compose down
git pull
docker compose up --build