package doss;

public class NoSuchBlobTxException extends BlobException {
    
    private static final long serialVersionUID = -3830901000693747244L;

    public NoSuchBlobTxException() {
    }

    public NoSuchBlobTxException(String message) {
        super(message);
    }

    public NoSuchBlobTxException(Throwable cause) {
        super(cause);
    }

    public NoSuchBlobTxException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoSuchBlobTxException(String message, Throwable cause,
            boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public NoSuchBlobTxException(Long txId) {
        super(txId.toString());
    }

}
