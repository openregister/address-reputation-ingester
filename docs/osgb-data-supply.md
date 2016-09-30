# OS-GB Data Supply

AddressBase-Premium is provided to HMRC according to the following agreed process. OS-GB provide a Webdav server containing files to be collected by HMRC automatically.

Overall, the following text describes the existing process offered by OS-GB in issuing AddressBase, with added specialisation to allow reliable automation of the file transfer.


## Requirements

Operating the process is must take little or no effort. The process must be resistant to human-induced errors; therefore it will be fully automatic (or as near fully automatic as possible). Therefore

1. The data on the webdav server must be self-describing. This will ensure that different products and/or versions don't get mixed up.

2. The data will consist of some rather large files. Therefore transfers will take a moderate amount of time and the transfer process must be resistant to race conditions.

3. The downloading software must ensure that there will not be any concurrent downloads that are able to corrupt any data due to the concurrency. (Perhaps this might be ensured by preventing more than one download at a time.)

4. Broadly, the process must be simple enough that it is easy to alter or enhance should future needs arise.

5. There is no particular need for high performance because this operates as a background batch function. Suffice it to say that excessively wasteful repetitiveness or high latencies are unwanted.


## Specification

1. There is a webdav server provided by OS-GB; HMRC will download data from it automatically.

2. There are several datasets held on the server.

3. Each dataset has a version history. Each version is called an epoch. Each epoch is indicated by a number that always increases over time.

4. We differentiate between 'full' product supply (i.e. the entire dataset) and change-only updates. In the current implementation, only full supply is supported.


### Directories

1. Each product is containined in a top-level folder with a suitable name. More products may be added later at this level. For now, the products are 

    * **abp** – Addressbase GB (England, Scotland, Wales)
    * **abi** – Addressbase Islands (NI, IoM, Guernsey, Jersey)

2. Each product directory contains one or more epoch datasets. The epoch is a number, as mentioned above.

3. Each epoch contains the supply type. We only implement '**full**' supply at the moment.

In summary, the naming pattern is "**product/epoch/variant**", for example: `abp/41/full`


### Content

1. Each new dataset is copied into the new 'full' folder, named as above. This may contain subdirectories, for example "data", "doc" and "resources".

2. The primary datafiles are CSV documents that are zipped. The filenames end with `.zip`. The names of the files must be such that, when sorted alphabetically and processed in that order, the sequence of the contained data will be delivered in the intended sequence. The details of the contained data are described in the AddressBase-Premium Technical Specification.

3. When upload of all the files of a dataset is complete, an empty marker file is created called `ready-to-collect.txt`. This must be within the 'full' folder. It must not be present until the upload has fully completed, so that race conditions are prevented.

4. Besides the files described above, it is possible for there to be other files or folders in any of the folders; they will essentially be ignored by download software.

5. When a new epoch is uploaded, no changes will be made to older epochs, except that the oldest epochs may be culled (deleted). Typically, the server will contain the current epoch and the previous epoch for every product.


### Remarks

It may be obvious from the above that incomplete uploads will be ignored by the downloader; only when the `ready-to-collect.txt` file is present can download commence.


### Example Directory Structure

The structure might look like this, which illustrates one epoch of 'abi' and two of 'abp', all of them are ready to collect.

```
/
  abi/
    42/
      full/
        ... lots of zipped csv files
        ready-to-collect.txt

  abp/
    41/
      full/
        data/
          ... lots of zipped csv files
        doc/
          ... anything here you want
        resources/
          ... metadata
        ready-to-collect.txt
    42/
      full/
        data/
          ... lots of zipped csv files
        doc/
          ... anything here you want
        resources/
          ... metadata
        ready-to-collect.txt
```

In this example, the abi folder above does not have subdirectories, whereas the other two products do - this is quite acceptable. 

