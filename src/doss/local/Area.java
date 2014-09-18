package doss.local;

import java.io.IOException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import doss.Blob;
import doss.core.Writables;

public class Area implements AutoCloseable {

    private final Database db;
    private final String name;
    private final List<Filesystem> filesystems;
    private final String containerType;
    private final Path root;
    private long maxContainerSize = 10L * 1024 * 1024 * 1024;
    private Container currentContainer;

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
        root = filesystems.get(0).path();
        currentContainer = null;
    }

    /**
     * Sets at which the container will be sealed.
     */
    public void setMaxContainerSize(long size) {
        this.maxContainerSize = size;
    }

    @Override
    public void close() {
        if (currentContainer != null) {
            currentContainer.close();
        }
    }

    /**
     * The container new blobs should be packed into.
     * 
     * @throws IOException
     */
    public synchronized Container currentContainer() throws IOException {
        if (currentContainer != null
                && currentContainer.size() > maxContainerSize) {
            currentContainer.close();
            db.sealContainer(currentContainer.id());
            currentContainer = null;
        }
        if (currentContainer == null) {
            Long containerId = db.findAnOpenContainer(name);
            if (containerId == null) {
                containerId = db.createContainer(name);
            }
            currentContainer = new DirectoryContainer(containerId,
                    root.resolve(Long.toString(containerId)));
        }
        return currentContainer;
    }

    /**
     * The container with the specified container id.
     * 
     * @throws IOException
     */
    public synchronized Container container(Long containerId)
            throws IOException {
        return new DirectoryContainer(containerId, root.resolve(Long
                .toString(containerId)));
    }

    /**
     * Returns the name of this area. eg "area.staging"
     */
    public String name() {
        return name;
    }

    // TODO: failure handling, journaling?
    void moveContainerFrom(Area srcArea, long containerId) throws IOException {
        try (Container in = srcArea.container(containerId);
                Container out = container(containerId)) {
            // copy container
            for (Blob blob : in) {
                out.put(blob.id(), Writables.wrap(blob));
            }

            // verify copy of container
            Iterator<Blob> inIt = in.iterator();
            Iterator<Blob> outIt = out.iterator();
            while (inIt.hasNext() && outIt.hasNext()) {
                Blob inBlob = inIt.next();
                Blob outBlob = outIt.next();
                assert inBlob.id() == outBlob.id();
                assert inBlob.size() == outBlob.size();
                try {
                    assert inBlob.digest("SHA-1").equals(
                            outBlob.digest("SHA-1"));
                } catch (NoSuchAlgorithmException e) {
                    throw new IOException(e);
                }
            }

            // update index
            db.updateContainerArea(containerId, name());

            // remove the old container
            in.permanentlyDelete();
        }

    }

    List<Filesystem> filesystems() {
        return Collections.unmodifiableList(filesystems);
    }
}
