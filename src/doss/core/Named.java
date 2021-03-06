package doss.core;

/**
 * An object that has an identifier.
 */
public interface Named {

    /**
     * Returns an identifier for this object.
     * 
     * @return the object's identifier
     */
    public abstract long id();

}