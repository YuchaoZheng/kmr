# kmr
mapreduce on k8s

# How to build
```bash
dep ensure -v
sudo mkdir -p /etc/kmr
cp deploy/local-config.json /etc/kmr/config.json

cd example/wordcount_new
go build example.go
```

- Run locally

```bash
./example master
# to set worker num
./example master --worker-num=10
```

- Run on k8s

Before deploy, there should be a available k8s and docker environment in local. 
make sure
`docker version` and `kubectl get pods` works well.
```bash
sudo ./example deploy
```