#  address-reputation-ingester

[![Build Status](https://travis-ci.org/hmrc/address-reputation-ingester.svg?branch=master)](https://travis-ci.org/hmrc/address-reputation-ingester) [![Download](https://api.bintray.com/packages/hmrc/releases/address-reputation-ingester/images/download.svg)](https://bintray.com/hmrc/releases/address-reputation-ingester/_latestVersion)

This application imports [AddressBase-Premium](https://www.ordnancesurvey.co.uk/business-and-government/products/addressbase-premium.html)
data from Ordnance Survey GB. It regularly check for updates and, when necessary, downloads and converts the data to the format 
used by the address-reputation microservice, which in turn feeds diverse systems within HMRC.

The application is built upon the [Play Framework using Scala](https://www.playframework.com/documentation/2.3.x/ScalaHome).

Timed behaviour is implemented using *cron* externally by poking the URL /goAuto/to/db

A simple built-in console allows viewing of the current status and triggering or cancelling various processing steps, as required.

## Elasticsearch Setup

You need Elasticsearch for development. Ubuntu example:

```
SOURCE=/etc/apt/sources.list.d/elasticsearch-2.x.list
echo "deb https://packages.elastic.co/elasticsearch/2.x/debian stable main" | sudo tee $SOURCE
apt-get -q -q update
apt-get -y install default-jdk elasticsearch
echo "cluster.name: address-reputation" | sudo tee -a /etc/elasticsearch/elasticsearch.yml
```

### Associated Repos

* [hmrc/addresses](https://github.com/hmrc/addresses) - documentation
* [hmrc/address-lookup](https://github.com/hmrc/address-lookup) - address-lookup microservice


### Licence

This code is open source software licensed under the [Apache 2.0 License]("http://www.apache.org/licenses/LICENSE-2.0.html").
    
