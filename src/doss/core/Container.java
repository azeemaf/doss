package doss.core;

import java.io.IOException;
import doss.Blob;
import doss.Writable;

public interface Container extends AutoCloseable {

    public Blob get(long offset) throws IOException;

    public long put(String id, Writable output)
            throws IOException;
    
    void close();
}