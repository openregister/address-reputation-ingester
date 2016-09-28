# Abpprocess

## Requirements
A local postgres installation with trust authentication enabled and the default 'postgres' database 


## Installation

Build the gem:


```ruby
$ gem build abpprocess.gemspec
```

Install it:
```
$ gem install abpprocess-0.1.0.gem
```

And then execute:

    $ abpprocess

    $ gem install abpprocess

## Usage
```
Usage: abpprocess [options] [product_and_ver] [files]
    -r                               Remove unzipped CSV files
    -c                               Generate type CSVs only
    -p                               Import only - assumes existing types CSVs
```

Examples:
```
abpprocess abp_43                       # Process zips in current dir into postgres
abpprocess abi_43 -c /tmp/*.zip         # Process zips in /tmp, creating type CSVs only
```