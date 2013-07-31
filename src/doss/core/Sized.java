package doss.core;

import java.io.IOException;

public interface Sized {

    /**
     * Returns the number of bytes in this object.
     * 
     * @throws IOException
     *             if an I/O occurs
     */
    public long size() throws IOException;

}