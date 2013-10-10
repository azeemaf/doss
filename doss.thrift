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

    PutHandle beginPut(1:BlobTxId txId),
    void write(1:PutHandle putHandle, 2:binary data),
    BlobId finishPut(1:PutHandle putHandle),
}