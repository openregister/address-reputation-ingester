This is how we got [HMRC's AddressBase matching
microservice](https://github.com/hmrc/address-reputation-ingester) working on an
Arch linux laptop.

## The git bit

Clone `git@github.com:hmrc/address-reputation-ingester.git` and `cd` into it.

### The AddressBase bit

Download the AddressBase data as a single zip, called e.g. `AB76GB_CSV.zip`,
according to the https://github.com/openregister/addressbase2postgres README.

Place it in some subdirectory `/wherever/you/like/abi/45/full/`.  Then edit
the file `conf/application.conf` in the `address-reputation-ingester` repo, to
change the line

```
    downloadFolder = "$HOME/OSGB-AddressBasePremium/download"
```

to

```
    downloadFolder = "/wherever/you/like"
```

The `/abi/45/full` bit is constructed by the UI according to what you tell it is
the "Product" (defaults to "abi" for "AddressBase Islands", even though what
we're using is "AddressBase Premium"), the "Epoch" (defaults to 45), and the
"Variant" (defaults to "full").

### The elasticsearch bit

We need a version 2 series of elasticsearch.  Fortunately the AUR has one.

```sh
yaourt -S elasticsearch2
sudo systemctl start elasticsearch.service
sudo systemctl enable elasticsearch.service
echo "cluster.name: address-reputation" | sudo tee -a /etc/elasticsearch/elasticsearch.yml
```

## The scala bit

```sh
sudo pacman -S scala sbt
```

I had to downgrade `ncurses` first to `ncurses-6.0+20170429-1-x86_64.pkg.tar.xz`

```sh
downgrade ncurses
```

After downgrading, `sbt` (in the root of the address-reputation-ingester) worked
(wait for ages).

```sh
sbt
run
```

## The ingestion bit

Go to http://localhost:9000/ and wait ages for it to load.

Click "Ingest only".

Your CPU should burn up for about 3 hours.  Click "Current status" to track
progress, or `tail -f address-reputation-ingester.log`.

It's done when you see something like

```text
...
Reading zip entry data/AddressBasePremium_FULL_2017-11-13_340.csv...
Reading zip entry data/AddressBasePremium_FULL_2017-11-13_341.csv...
Reading zip entry resources/AddressBase_products_classification_scheme.csv...
Reading zip entry resources/Record_10_HEADER_Header.csv...
Reading zip entry resources/Record_11_STREET_Header.csv...
Reading zip entry resources/Record_15_STREETDESCRIPTOR_Header.csv...
Reading zip entry resources/Record_21_BLPU_Header.csv...
Reading zip entry resources/Record_23_XREF_Header.csv...
Reading zip entry resources/Record_24_LPI_Header.csv...
Reading zip entry resources/Record_28_DELIVERYPOINTADDRESS_Header.csv...
Reading zip entry resources/Record_29_METADATA_Header.csv...
Reading zip entry resources/Record_30_SUCCESSOR_Header.csv...
Reading zip entry resources/Record_31_ORGANISATION_Header.csv...
Reading zip entry resources/Record_32_CLASSIFICATION_Header.csv...
Reading zip entry resources/Record_99_TRAILER_Header.csv...
Reading from 354 CSV files in AB76GB_CSV.zip (1 of 1) took 8968s.
Second pass processed 28816409 DPAs, 4040001 LPIs.
Ingester finished after 9539s.
Finished ingesting to index abi_45_201801101001
Cleaning up the ingester: ok.
Finished ingesting to es abi/45/full after 9539s.
idle
```

That created an index, so set the thing to use that index by clicking "List" to
show the index's name, pasting the name into the "Index" box, and clicking
"Switch to".  You'll probably see this:

```text
Starting switching to abi_45_201801101001.
Warn: abi_45_201801101001: index is still being written
Finished switching to abi_45_201801101001 after 12.6ms.
~~~~~~~~~~~~~~~ 10-Jan-2018 12:47:20 ~~~~~~~~~~~~~~~
Starting switching to abi_45_201801101001.
Warn: abi_45_201801101001: index is still being written
Finished switching to abi_45_201801101001 after 5945μs.
idle
```

That means it didn't work!  So bodge it.  Edit `app/controllers/SwitchoverController.scala` line 67 from

```scala
else if (indexMetadata.findMetadata(newName).exists(_.completedAt.isDefined)) {
```

to

```scala
else if (true) {
```

## The address-lookup bit

Clone git@github.com:hmrc/address-lookup.git.  `cd` into it, start `sbt`, and
`run 9022` (or some port number that is available).

The `/v2/uk/addresses` endpoint is quite fussy about optional parameters.  We
think the rules are:

1. Unless you provide `postcode`, don't provide `filter`
2. If you only provide `postcode`, don't provide `limit`

`curl --header "X-LOCALHOST-Origin: 0" 'http://localhost:9022/v2/uk/addresses?postcode=AA1+1AA'`
`curl --header "X-LOCALHOST-Origin: 0" 'http://localhost:9022/v2/uk/addresses?postcode=HA6+3BF&filter=46+Rofant+Road'`
`curl --header "X-LOCALHOST-Origin: 0" 'http://localhost:9022/v2/uk/addresses?postcode=HA6+3BF'`
`curl --header "X-LOCALHOST-Origin: 0" 'http://localhost:9022/v2/uk/addresses?postcode=HA6+3BE&filter='`
`curl --header "X-LOCALHOST-Origin: 0" 'http://localhost:9022/v2/uk/addresses?postcode=HA6+3BE&limit='`
`curl --header "X-LOCALHOST-Origin: 0" 'http://localhost:9022/v2/uk/addresses?postcode=HA6+3BE&limit=1'`
`curl --header "X-LOCALHOST-Origin: 0" 'http://localhost:9022/v2/uk/addresses?postcode=HA6+3BE&line1=46'`
`curl --header "X-LOCALHOST-Origin: 0" 'http://localhost:9022/v2/uk/addresses?postcode=HA6+3BE&line1=46&limit=1'`
`curl --header "X-LOCALHOST-Origin: 0" 'http://localhost:9022/v2/uk/addresses?postcode=HA6+3BE&line2=46&limit=1'`
`curl --header "X-LOCALHOST-Origin: 0" 'http://localhost:9022/v2/uk/addresses?filter=HA6+3BE'`
`curl --header "X-LOCALHOST-Origin: 0" 'http://localhost:9022/v2/uk/addresses?line2=46&limit=1'`
`curl --header "X-LOCALHOST-Origin: 0" 'http://localhost:9022/v2/uk/addresses?limit=1'`
`curl --header "X-LOCALHOST-Origin: 0" 'http://localhost:9022/v2/uk/addresses?line1=1'`
`curl --header "X-LOCALHOST-Origin: 0" 'http://localhost:9022/v2/uk/addresses?postcode=HA6+3BE&line1=46&filter='`
`curl --header "X-LOCALHOST-Origin: 0" 'http://localhost:9022/v2/uk/addresses?line1=46&filter='`

# Multiple matches

This returns many matches because it doesn't use any information about the place
being a jobcentre.  The UPRN we chose for the jobcentre register was
200003771457.

`curl --header "X-LOCALHOST-Origin: 0" 'http://localhost:9022/v2/uk/addresses?postcode=cf44%207hu&line1=crown%20buildings&line2=greenbach&town=aberdare' | jq .`

## Improvements to make

* Align the parts of an address in the index (`line1`, `postcode`, etc.) to the
    fields returned by libpostal, which normalises free-form addresses.
