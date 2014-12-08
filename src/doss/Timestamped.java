package doss;

import java.io.IOException;
import java.nio.file.attribute.FileTime;

public interface Timestamped {

    /**
     * Returns the object's creation date.
     *
     * @throws IOException
     *             if an I/O error occurs
     */
    public FileTime created() throws IOException;

}
