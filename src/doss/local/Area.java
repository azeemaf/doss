package doss.local;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class Area implements AutoCloseable {

    private final String name;
    private final List<Filesystem> filesystems;
    private final String containerType;
    private final Container container;

    public Area(String name, List<Filesystem> filesystems, String containerType)
            throws IOException {
        this.name = name;
        this.filesystems = filesystems;
        this.containerType = containerType;
        if (filesystems.size() != 1) {
            throw new IllegalArgumentException(name
                    + ": currently only a single fs per area is implemented");
        }
        if (!Objects.equals(containerType, "directory")) {
            throw new IllegalArgumentException(
                    name
                            + ": currently only directory container type is supported, not "
                            + containerType);
        }
        container = new DirectoryContainer(0, filesystems.get(0).path());
    }

    @Override
    public void close() {
        container.close();
    }

    /**
     * The container new blobs should be packed into.
     */
    public Container currentContainer() {
        return container;
    }

}
