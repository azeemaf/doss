package doss.db;

import java.nio.file.NoSuchFileException;

public class NoSuchBlobException extends NoSuchFileException {
    private static final long serialVersionUID = 3379334544710041568L;

    public NoSuchBlobException(long blobId) {
        super(Long.toString(blobId));
    }

}
