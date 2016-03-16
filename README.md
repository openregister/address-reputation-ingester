
# address-lookup-ingester

[![Build Status](https://travis-ci.org/hmrc/address-reputation-ingester.svg?branch=master)](https://travis-ci.org/hmrc/address-reputation-ingester) [ ![Download](https://api.bintray.com/packages/hmrc/releases/address-reputation-ingester/images/download.svg) ](https://bintray.com/hmrc/releases/address-reputation-ingester/_latestVersion)

This is an application that imports AddressBase-Premium data from Ordnance Survey GB. It regularly check for updates and, when
necessary, downloads and converts the data to the format used by the address-reputation microservice, which in turn feeds
diverse systems within HMRC.

The application is built upon the Play Framework using Scala.

### License

This code is open source software licensed under the [Apache 2.0 License]("http://www.apache.org/licenses/LICENSE-2.0.html").
    
