docker rmi distboruvka
cd cmd/node
go build -race
cd ../..
docker build -t distboruvka .
