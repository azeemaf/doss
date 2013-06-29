package doss;

public class BlobException extends RuntimeException {

    private static final long serialVersionUID = 3448179096170935730L;

    public BlobException() {
    }

    public BlobException(String message) {
        super(message);
    }

    public BlobException(Throwable cause) {
        super(cause);
    }

    public BlobException(String message, Throwable cause) {
        super(message, cause);
    }

    public BlobException(String message, Throwable cause,
            boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
