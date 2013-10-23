package doss.local;

import java.io.IOException;

import doss.Blob;
import doss.Writable;
import doss.core.Named;

/**
 * A container (usually a tar file) is a sequence of blobs that have been
 * concatenated together so that an underlying filesystem sees them as a single
 * file.
 * 
 * In normal operation a BlobStore will have many containers. New blobs are
 * appended to an open container in the staging filesystem until it reaches a
 * configured size limit. The container is then sealed and then moved to the
 * preservation filesystem.
 * 
 * Motivation: By packing blobs into containers the underlying filesystem only
 * has to deal with thousands of container files rather than the billions of
 * individual blobs. This allows us to use archival filesystems which are
 * typically optimized for storing small numbers of large files. It also means
 * we can do bulk operations like media migrations and digest (checksum)
 * verification at a whole container level as most storage hardware is far more
 * efficient at large sequential reads and writes than small random ones.
 */
public interface Container extends AutoCloseable, Named {

    public Blob get(long offset) throws IOException;

    public long put(long blobId, Writable output) throws IOException;

    @Override
    void close();
}