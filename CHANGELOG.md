# OSGL Storage CHANGE LOG 

1.8.0 - 30/Oct/2018
* update to osgl-tool-1.18.0

1.7.0 - 14/Jun/2018
* update aws-java-sdk-s3 to 1.11.347
* update to osgl-tool-1.15.1

1.6.0 - 19/May/2018
* update osgl-tool to 1.13.1
    - move SObject, ISObject and IStorageService to osgl-tool

1.5.4 - 19/May/2018
* update osgl-tool to 1.13.0
* update aws-sdk to 1.11.333

1.5.3 - 02/Apr/2018
* update osgl-tool to 1.9.0

1.5.2 - 25/Mar/2018
* update osgl-tool to 1.8.1

1.5.1 - 25/Mar/2018
* update to osgl-1.8.0
* update aws-sdk to 1.11.301
* update azure-storage to 7.0.0
* update spring to 3.2.18.RELEASE

1.5.0
* update to osgl-1.5

1.4.2
* improve maven build process
* apply osgl-bootstrap versioning
* add badges to README
* apply javadoc stylesheet


1.4.1
* It reports `NullPointerException` when trying to load an SObject from persistent storage without attribute file #21 
* Missing attributes file since 1.4.0 #20 
* "Cannot create dir" on 1.4.0 with FileSystemService #19 

1.4.0
* Optimize FileObject implementation #18 
* Add getFile(String fullPath) protected method into FileSystemService #17 
* Add getter and setter for filename, contentType on ISObject #16 
* It cannot determine `.png` file's mime type #15 
* S3 allow only US-ASCII characters in the meta data #14 
* DumbObject shall return valid url in getUrl call #13 


1.3.0
* Add factory method to load `SObject` from class path #12 

1.2.0
* Enable user defined `KeyGenerator` #11 

1.1.1
* NPE triggered when loading storage object without attr file #10 

1.1.0
* #8 Allow store sobject with suffix attached to the key
* #9 Allow plugin key name provider and key generator

1.0.1
* take out version range. See https://issues.apache.org/jira/browse/MNG-3092

1.0.0
* baseline from 0.8

0.8.0-SNAPSHOT
* update tool to 0.10.0
* update logging to 0.7.0

0.7.3-SNAPSHOT
* Fix issue: UnsupportedException caused by StorageServiceBase.put trying to access sobj.length()
             when sobj is an instance of InputStreamSObject

0.7.2-SNAPSHOT
* Update aws to 1.11.31
* Update azure to 4.4.0

0.7.1-SNAPSHOT
* FileSystem service always init root_ to null when started up #6 
* add getUrl() to ISObject #7 

0.7.0-SNAPSHOT
* Put azure sobject might cause 0 length object be saved on the cloud #4 
* Refactor azure/aws service/sobj implementation #5 

0.6.1-SNAPSHOT
* support Azure Blob service

0.6.0-SNAPSHOT
* upgrade to osgl-tool 0.9
* support context path

0.5.3-SNAPSHOT
* upgrade to osgl-tool 0.7.1-SNAPSHOT

0.5.2-SNAPSHOT
- Add new configuration items for S3 storage:
* CONF_MAX_ERROR_RETRY
* CONF_CONN_TIMEOUT
* CONF_SOCKET_TIMEOUT
* CONF_TCP_KEEP_ALIVE
* CONF_MAX_CONN

0.5.1-SNAPSHOT
- Add isDump() method to ISObject interface

0.5.0-SNAPSHOT
- IStorageService.put now returns an ISObject instance representing the persisted data

0.4.2-SNAPSHOT
- Use SoftReference to wrap byte array buffer for FileObject and S3Obj to prevent potential memory issue

0.4.1-SNAPSHOT
- Fix issue when saving loaded SObject from file back it result in an empty file

0.4-SNAPSHOT
- Upgrade to osgl-tool 0.4-SNAPSHOT

0.3-SNAPSHOT
- base version when history log started
