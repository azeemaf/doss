package doss.local;


    
    public class TarContainerException extends RuntimeException {

        private static final long serialVersionUID = 3448179096170935731L;

        
        public TarContainerException() {
        }

        
        public TarContainerException(String message) {
            super(message);
        }

        
        public TarContainerException(Throwable cause) {
            super(cause);
        }

        public TarContainerException(String message, Throwable cause) {
            super(message, cause);
        }

        public TarContainerException(String message, Throwable cause,
                boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
   

}
