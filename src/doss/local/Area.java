package doss.local;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public class Area implements AutoCloseable {

    private final Database db;
    private final String name;
    private final List<Filesystem> filesystems;
    private final String containerType;
    private final Container container;
    private final long maxContainerSize = 10 * 1024 * 1024 * 1024;

    public Area(Database db, String name, List<Filesystem> filesystems,
            String containerType) throws IOException {
        this.db = db;
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
        Path root = filesystems.get(0).path();
        Long containerId = db.findAnOpenContainer(name);
        if (containerId == null) {
            containerId = db.createContainer(name);
        }
        container = new DirectoryContainer(containerId,
                root.resolve(containerId.toString()));
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
