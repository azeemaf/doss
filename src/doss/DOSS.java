package doss;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import doss.local.LocalBlobStore;

public class DOSS {

    public static BlobStore openLocalStore(Path root) throws IOException {
        return new LocalBlobStore(root);
    }

}
