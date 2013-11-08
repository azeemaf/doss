package doss;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.nio.file.Paths;

import doss.local.LocalBlobStore;
import doss.net.RemoteBlobStore;

public class DOSS {
    public static BlobStore open(String url) throws IOException {
        URI uri = URI.create(url);
        String scheme = uri.getScheme();
        if (scheme.equals("file")) {
            return LocalBlobStore.open(Paths.get(uri));
        } else if (scheme.equals("doss")) {
            Socket socket = new Socket(uri.getHost(), uri.getPort());
            return RemoteBlobStore.open(socket);
        }
        throw new IllegalArgumentException("Unknown URL scheme '"
                + uri.getAuthority() + "'. Must be file:// or doss://");
    }
}
