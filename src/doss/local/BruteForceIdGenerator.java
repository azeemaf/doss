package doss.local;

import java.io.FileNotFoundException;

import doss.BlobStore;
import doss.IdGenerator;

/**
 * An exceedingly slow and silly id generator that just repeatedly increments an
 * long until it finds no corresponding object in the BlobStore.
 * 
 * Just a placeholder until we implement something better.
 */
public class BruteForceIdGenerator implements IdGenerator {

    private BlobStore blobStore;

    public BruteForceIdGenerator(BlobStore blobStore) {
        this.blobStore = blobStore;
    }

    @Override
    public String generate() {
        Long id = 0L;
        try {
            while (blobStore.get(id.toString()) != null) {                
                id++;
            }
        } catch (FileNotFoundException e) {
            // found one
        }
        return id.toString();
    }

}
