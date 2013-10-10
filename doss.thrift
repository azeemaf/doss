namespace java doss.net

struct StatResponse {
    1:required i64 blobId,
    2:required i64 size,
    3:optional i64 createdMillis,
}

exception RemoteNoSuchBlobException {
    1:required i64 blobId,
}

exception RemoteIOException {
    1:optional i64 blobId,
    2:optional string type,
    3:optional string messsage,
}

service DossService {
    StatResponse stat(1:i64 blobId)
        throws (1:RemoteNoSuchBlobException noSuchBlobException,
                2:RemoteIOException ioException),

    binary read(1:i64 blobId, 2:i64 offset, 3:i32 length)
        throws (1:RemoteNoSuchBlobException noSuchBlobException,
                2:RemoteIOException ioException),

    i64 beginTx(),
}