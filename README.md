DOSS 2
======

<a href="https://secure.flickr.com/photos/ubclibrary/3676876746/"><img src="https://farm3.staticflickr.com/2626/3676876746_7b20ff04ed_n.jpg" align="right" alt="UBC Library Robot"></a>

DOSS 2 will be an object storage abstraction layer with transactional
write-once semantics.  Objects are uniquely and persistently
identified and can never be overwritten only created and (soft)
deleted.

[![Build Status](https://travis-ci.org/nla/doss.png?branch=master)](https://travis-ci.org/nla/doss)

**Status**

* Java API: unstable
* Local backend: incomplete
* Network backend: to be started
* [CLI frontend](https://github.com/nla/doss/issues/5): incomplete
* [FUSE frontend](doc/native-interface.md): to be started
* [Container support](doc/archive-formats.md): to be started

Feature Goals
-------------

* write once blob storage semantics
* concurrent distributed random read access
* multiple storage pools and devices (with different policies)
* two-phase commit write transactions
* soft deletes ("rubbish bin")
* consistency and integrity checks
* packs small files into containers for efficient bulk processing

Java API
--------

See [java-api.md](doc/java-api.md) for more examples.

### Ingesting files

```java
try (BlobStore bs = DOSS.openLocalStore("/doss-devel");
     BlobTx tx = bs.begin()) {    
    Blob blob1 = tx.put(Paths.get("/tmp/myimage.jpg"));
    Blob blob2 = tx.put(Paths.get("/tmp/mytext.txt"));
    blob2.verifyDigest("SHA1", "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed");
    tx.commit();
    return blob1.id();
}
```

### Random access

```java
try (BlobStore bs = DOSS.openLocalStore("/doss-devel")) {
    Blob blob = bs.get("962b6910");
    try (Channel channel = blob.openChannel()) {
        // do something with the channel
    }
}
```

Non-Java read access
--------------------

See [doc/native-interface.md](doc/native-interface.md).

```sh
dossfs /doss-devel /mnt
grep -i endeavour /mnt/962b6910
convert /mnt/014908b0 /tmp/kangaroo.jpg
```

Building
--------

Requirements:

* Java 7
* Maven 3

Compile by typing:

    mvn package

License
-------

Copyright 2013 National Library of Australia

DOSS 2 is free software; you can redistribute it and/or modify it under
the terms of the Apache License, Version 2.0:

http://www.apache.org/licenses/LICENSE-2.0

See the file `LICENSE` for details.
