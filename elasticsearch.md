# Elastic Search
## Developer Notes - The Docker Way

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

## Or Directly on the OS

See [Installation](https://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html)

