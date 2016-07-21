# Elastic Search
## Developer Notes

The easiest way to get started is via Docker (if you already have Docker installed)

```
docker pull elasticsearch:2
```
then
```
mkdir -p esdata
docker run -d -v "$PWD/esdata":/usr/share/elasticsearch/data elasticsearch
```
([docs](https://hub.docker.com/_/elasticsearch/))
