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
     * @return the object's creation date
     * @throws IOException if an I/O occurs
     */
    public abstract FileTime lastModifed() throws IOException;

    /**
     * Returns the last access date for this object.
     * 
     * @return the object's creation date
     * @throws IOException if an I/O occurs
     */
    public abstract FileTime lastAccess() throws IOException;

    
}