DOSS Java API
=============

These examples use `DOSS.openLocalStore` which opens a local directory
as a read-write data store.  In a large-scale production system the
majoriy of clients would only have read-only access and writes would
be performed via a remote privileged daemon that authenticates the
clients.

### Reading sequentially

```java
try (BlobStore bs = DOSS.openLocalStore("/doss-devel")) {
    Blob blob = bs.get("962b6910");
    try (InputStream stream = blob.openStream()) {
        return ImageIO.read(stream);
    }
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

### Ingesting using a channel

You may want to ingest from somewhere other than the local filesystem
such as a socket or a serialized document that's constructed on the
fly.  The `put(ChannelOutput)` method allows you to stream output to a
blob using a channel.

```java
try (BlobTx tx = bs.begin()) {

    Blob blob = tx.put(new ChannelOutput() {

        void write(WritableByteChannel out) {
            XMLOutputFactory factory = XMLOutputFactory.newInstance();
            try (OutputStream outputStream = Channels.newOutputStream(out);
                 XMLStreamWriter xml = factory.createXMLStreamWriter(outputStream)) {
                xml.writeStartDocument();
                xml.writeStartElement("h1");
                xml.writeCharacters("hello world");
                xml.writeEndElement();
                xml.writeEndDocument();
                xml.flush();
            }
        }

    });

    tx.commit();

    return blob.id();
}
```

### Two-phase commit

The [two-phase commit protocol](https://en.wikipedia.org/wiki/Two-phase_commit_protocol)
enables DOSS to participate in distributed transactions.  There are
other uses for this but the primary goal is that a DOSS blobstore can
be kept in lockstep with an SQL database storing object metadata.

This is an example of how that could be implemented using an extra
journal table in the SQL database.

```java
try (BlobTx tx = bs.begin()) {    

    Blob blob = tx.put(Paths.get("/tmp/myimage.jpg"));

    // add data transaction to SQL journal for rollback on crash recovery
    db.createStatement("insert into journal(tx_id) values (:tx, 'rollback')")
        .bind(0, tx.id()).execute();

    // finish uploading data and ensure everything is green
    tx.prepare();

    try {
        database.inTransaction(new TransactionCallback<Integer>() {
            public Integer inTransaction(Handle h, TransactionStatus status) {
     
                // record some metadata about this file
                h.createStatement("insert into pictures (desc, id) values (:desc, :id)"
                    .bind(0, "my cool picture")
                    .bind(1, blob.id())
                    .execute();
     
                // update journal to commit on crash recovery
                h.createStatement("update journal set action = 'commit' where tx_id = :tx")
                    .bind(0, tx.id())
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
void finish(BlobTx tx) {
    String action = db.createQuery("select action from journal where tx_id = :tx")
        .bind("tx", tx.id())
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
        .bind("tx", tx.id())
        .execute();
}
```

On startup or connection re-establishment after a database/blobstore
outage the application should recover and finish any interrupted
transactions:

```java
for (BlobTx tx : bs.recover()) {
    finish(tx);
}
```
