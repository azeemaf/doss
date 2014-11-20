package doss.local;

public class NoSuchContainerException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public NoSuchContainerException() {
    }

    public NoSuchContainerException(Long containerId) {
        super(containerId.toString());
    }

    public NoSuchContainerException(Throwable cause) {
        super(cause);
    }

    public NoSuchContainerException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoSuchContainerException(String message, Throwable cause,
            boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
