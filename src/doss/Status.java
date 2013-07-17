package doss;

import java.io.IOException;
import java.nio.file.attribute.FileTime;

/**
 * An object that has status, like dates, etc, 
 */
public interface Status extends Sized {

    /**
     * Returns the creation date for this object.
     * 
     * @return the object's creation date
     * @throws IOException if an I/O occurs
     */
    public abstract FileTime created() throws IOException;

    /**
     * Returns the modification date for this object.
     * 
     * @return the object's last modification date
     * @throws IOException if an I/O occurs
     */
    public abstract FileTime lastModified() throws IOException;

    /**
     * Returns the last access date for this object.
     * 
     * @return the object's access date
     * @throws IOException if an I/O occurs
     */
    public abstract FileTime lastAccess() throws IOException;

    /**
     * Returns an object that uniquely identifies the given file, or null if a file key is not available.  This is commonly the inode or device id.
     * 
     * @return 
     * @throws IOException if an I/O occurs
     */
    public abstract Object fileKey() throws IOException;

    
}