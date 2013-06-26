package doss;

import java.io.IOException;

public interface IdGenerator {
    
    /**
     * Generates a new storage identifier
     * @return a newly minted id
     * @throws IOException if an I/O occurs
     */
    long generate() throws IOException;
    
}
