package doss;

import java.io.IOException;
import java.nio.file.Path;

public class CorruptBlobStoreException extends BlobException {

    private static final long serialVersionUID = 7964087453644609378L;

    public CorruptBlobStoreException(Path root, IOException e) {
        super("blob store missing or corrupt: " + root, e);
    }

}
