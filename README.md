DOSS 2
======

Usage
-----

The interface hasn't been written yet, but here's roughly what it
should look like.

### Reading sequentially

```java
try (DataStore ds = DOSS.openLocalStore("/doss-devel")) {
    Blob blob = ds.get("962b6910-b3eb-11e2-9e96-0800200c9a66");
    try (InputStream steam = ds.openStream()) {
        return ImageIO.read(stream);
    }
}
```

### Random access

```java
try (DataStore ds = DOSS.openLocalStore("/doss-devel")) {
    Blob blob = ds.get("962b6910-b3eb-11e2-9e96-0800200c9a66");
    try (Channel channel = ds.openChannel()) {
        // do something with the channel
    }
}
```

### Ingesting files

```java
try (DataStore ds = DOSS.openLocalStore("/doss-devel");
     DataTxn tx = ds.begin()) {    
    Blob blob1 = tx.create(Paths.get("/tmp/myimage.jpg"));
    Blob blob2 = tx.create(Paths.get("/tmp/mytext.txt"));
    blob2.verifyDigest("SHA1", "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed");
    tx.commit();
    return blob1.getId();
}
```

### Ingesting via a channel

```java
try (DataStore ds = DOSS.openLocalStore("/doss-devel"); 
     DataTxn tx = ds.begin()) {
    Blob blob = tx.create();

    try (WritableByteChannel out = blob.openWriteChannel();
         FileChannel in = FileChannel.open(Paths.get("/tmp/myimage.jpg"))) {
        in.transferTo(out, 0, Long.MAX_VALUE);
    }

    tx.commit();
    return blob.getId();
}
```

### Two-phase commit

The [two-phase commit protocol](https://en.wikipedia.org/wiki/Two-phase_commit_protocol)
enables DOSS to participate in distributed transactions.  There are
other uses for this but the primary goal is that a DOSS datastore can
be kept in lockstep with an SQL database storing object metadata.

This is an example of how that could be implemented using an extra
journal table in the SQL database.

```java
try (DataTxn tx = ds.begin()) {    

    Blob blob = tx.create(Paths.get("/tmp/myimage.jpg"));

    // add data transaction to SQL journal for rollback on crash recovery
    db.createStatement("insert into journal(tx_id) values (:tx, 'rollback')")
        .bind(0, tx.getId()).execute();

    // finish uploading data and ensure everything is green
    tx.prepare();

    try {
        database.inTransaction(new TransactionCallback<Integer>() {
            public Integer inTransaction(Handle h, TransactionStatus status) {
     
                // record some metadata about this file
                h.createStatement("insert into pictures (desc, id) values (:desc, :id)"
                    .bind(0, "my cool picture")
                    .bind(1, blob.getId())
                    .execute();
     
                // update journal to commit on crash recovery
                h.createStatement("update journal set action = 'commit' where tx_id = :tx")
                    .bind(0, tx.getId())
                    .execute();
            }
        }
    } finally {
        finish(tx);
    }
}
```

Finishing the transaction should be done by reading the journal entry.
This ensures that even if an exception is thrown we finish based
solely on the outcome of the SQL transaction (ie no false rollbacks).

```java
void finish(DataTxn tx) {
    String action = db.createQuery("select action from journal where tx_id = :tx")
        .bind("tx", tx.getId())
        .map(StringMapper.FIRST)
        .first();

    if (action == null) {
        return; // someone else's transaction
    } else if (action.equals("commit")) {
        tx.commit();
    } else {
        tx.rollback();
    }

    h.createStatement("delete from journal where tx_id = :tx")
        .bind("tx", tx.getId())
        .execute();
}
```

On startup or connection re-establishment after a database/datastore
outage the application should recover and finish any interrupted
transactions:

```java
for (DataTxn tx : ds.recover()) {
    finish(tx);
}
```

### dossfs

```sh
dossfs /doss-devel /mnt
grep -i endeavour /mnt/962b6910-b3eb-11e2-9e96-0800200c9a66
convert /mnt/014908b0-b3f1-11e2-9e96-0800200c9a66 /tmp/kangaroo.jpg
```

Building
--------

Requirements:

* Java 7
* Maven 3

Compile by typing:

    mvn package
