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

Then edit /etc/elasticsearch/elasticsearch.yml and set
```
cluster.name: address-reputation
```

And in /etc/defaults/elasticsearch, set (e.g.)
```
ES_HEAP_SIZE=4G
```

Finally, add the Kopf plugin. This might be done by downloading the tgz and unpacking it in
/usr/share/elasticsearch/plugins.

