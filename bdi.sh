docker rmi distboruvka
cd cmd/node
go build
cd ../..
docker build -t distboruvka .
