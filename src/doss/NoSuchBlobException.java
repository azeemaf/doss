package doss;

public class NoSuchBlobException extends BlobException {

    private static final long serialVersionUID = 4400283547656303379L;

    public NoSuchBlobException() {
    }

    public NoSuchBlobException(Long blobId) {
        super(blobId.toString());
    }

    public NoSuchBlobException(Throwable cause) {
        super(cause);
    }

    public NoSuchBlobException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoSuchBlobException(String message, Throwable cause,
            boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
