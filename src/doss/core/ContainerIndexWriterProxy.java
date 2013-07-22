package doss.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides a single object that can call many {@link ContainerIndexWriter}
 * objects
 */
public class ContainerIndexWriterProxy implements ContainerIndexWriter {

    List<ContainerIndexWriter> indexes = new ArrayList<ContainerIndexWriter>();

    synchronized public void addContainerIndex(
            ContainerIndexWriter containerIndex) {
        indexes.add(containerIndex);
    }

    @Override
    synchronized public void remember(long blobId, long offset) {
        for (ContainerIndexWriter index : indexes) {
            index.remember(blobId, offset);
        }
    }

    @Override
    synchronized public void delete(long blobId) {
        for (ContainerIndexWriter index : indexes) {
            index.delete(blobId);
        }
    }

}
