package doss.local;

import java.io.IOException;
import java.util.Iterator;

/**
 * Admin tools. Should not be used by client applications. Potentially dangerous
 * operations and no interface stability guarantees.
 */
public class Admin {
    private final LocalBlobStore blobStore;

    public Admin(LocalBlobStore blobStore) {
        this.blobStore = blobStore;
    }

    public Iterable<Container> listContainers() throws IOException {
        return new Iterable<Container>() {
            Iterator<Long> idIt = blobStore.db.findAllContainers().iterator();

            @Override
            public Iterator<Container> iterator() {
                return new Iterator<Container>() {
                    @Override
                    public boolean hasNext() {
                        return idIt.hasNext();
                    }

                    @Override
                    public Container next() {
                        try {
                            return blobStore.stagingArea.container(idIt.next());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public void remove() {
                    }
                };
            }
        };
    }

    /**
     * Beware: only sets the flag in the database. Any active processes may keep
     * using the container.
     */
    public void sealContainer(long containerId) {
        blobStore.db.sealContainer(containerId);
    }
}
