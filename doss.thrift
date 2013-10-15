namespace java doss.net

typedef i64 BlobId
typedef i64 BlobTxId
typedef i64 PutHandle

struct StatResponse {
    1:required BlobId blobId,
    2:required i64 size,
    3:optional i64 createdMillis,
}

exception RemoteNoSuchBlobException {
    1:required BlobId blobId,
}

exception RemoteIOException {
    1:optional BlobId blobId,
    2:optional string type,
    3:optional string messsage,
}

service DossService {
    StatResponse stat(1:BlobId blobId)
        throws (1:RemoteNoSuchBlobException noSuchBlobException,
                2:RemoteIOException ioException),

    binary read(1:BlobId blobId, 2:i64 offset, 3:i32 length)
        throws (1:RemoteNoSuchBlobException noSuchBlobException,
                2:RemoteIOException ioException),

    BlobTxId beginTx(),
    void commitTx(1:BlobTxId txId),
    void rollbackTx(1:BlobTxId txId),
    void prepareTx(1:BlobTxId txId),

    BlobId put(1:BlobTxId txId, 2:binary data),
}