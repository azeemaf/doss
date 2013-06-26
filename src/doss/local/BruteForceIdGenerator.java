package doss.local;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

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
    public long generate() throws IOException {
        Long id = 0L;
        try {
            while (blobStore.get(id.toString()) != null) {                
                id++;
            }
        } catch (NoSuchFileException e) {
            // found one
        }
        return id;
    }

}
