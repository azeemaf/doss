package doss.local;

public class ContainerInUseException extends RuntimeException {

    public ContainerInUseException(long containerId) {
        super(Long.toString(containerId));
    }

    private static final long serialVersionUID = 1L;

}
