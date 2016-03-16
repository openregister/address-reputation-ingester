
# address-reputation-ingester

[![Build Status](https://travis-ci.org/hmrc/address-reputation-ingester.svg?branch=master)](https://travis-ci.org/hmrc/address-reputation-ingester) [ ![Download](https://api.bintray.com/packages/hmrc/releases/address-reputation-ingester/images/download.svg) ](https://bintray.com/hmrc/releases/address-reputation-ingester/_latestVersion)

This application imports [AddressBase-Premium](https://www.ordnancesurvey.co.uk/business-and-government/products/addressbase-premium.html)
data from Ordnance Survey GB. It regularly check for updates and, when
necessary, downloads and converts the data to the format used by the address-reputation microservice, which in turn feeds
diverse systems within HMRC.

The application is built upon the [Play Framework using Scala](https://www.playframework.com/documentation/2.3.x/ScalaHome).

Timed behaviour is implemented using *cron* externally.

### Licence

This code is open source software licensed under the [Apache 2.0 License]("http://www.apache.org/licenses/LICENSE-2.0.html").
    
