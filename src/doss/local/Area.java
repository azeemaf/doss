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
    private final Path root;
    private long maxContainerSize = 10L * 1024 * 1024 * 1024;
    private Container currentContainer;

    public Area(Database db, String name, List<Filesystem> filesystems, String containerType) throws IOException {
        this.db = db;
        this.name = name;
        this.filesystems = filesystems;
        this.containerType = containerType;
        if (filesystems.size() != 1) {
            throw new IllegalArgumentException(name + ": currently only a single fs per area is implemented");
        }
        if (!Objects.equals(containerType, "directory")) {
            throw new IllegalArgumentException(name + ": currently only directory container type is supported, not " + containerType);
        }
        root = filesystems.get(0).path();
        Long containerId = db.findAnOpenContainer(name);
        if (containerId == null) {
            containerId = db.createContainer(name);
        }
        currentContainer = new DirectoryContainer(containerId, root.resolve(containerId.toString()));
    }

    /**
     * Sets at which the container will be sealed.
     */
    public void setMaxContainerSize(long size) {
        this.maxContainerSize = size;
    }

    @Override
    public void close() {
        currentContainer.close();
    }

    /**
     * The container new blobs should be packed into.
     * 
     * @throws IOException
     */
    public synchronized Container currentContainer() throws IOException {
        if (currentContainer.size() > maxContainerSize) {
            currentContainer.close();
            db.sealContainer(currentContainer.id());
            long containerId = db.createContainer(name);
            currentContainer = new DirectoryContainer(containerId, root.resolve(Long.toString(containerId)));
        }
        return currentContainer;
    }

    /**
     * The container with the specified container id.
     * 
     * @throws IOException
     */
    public synchronized Container container(Long containerId) throws IOException {
        return new DirectoryContainer(containerId, root.resolve(Long.toString(containerId)));
    }
}
