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
the "Product" (defaults to "abi" for "AddressBase Premium"), the "Epoch"
(defaults to 45), and the "Variant" (defaults to "full").

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

## The prize

Go to http://localhost:9000/ and wait ages for it to load.

Click "Ingest only".

Your CPU should burn up.
