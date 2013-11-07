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
        if (uri.getAuthority().equals("file")) {
            return LocalBlobStore.open(Paths.get(uri));
        } else if (uri.getAuthority().equals("doss")) {
            Socket socket = new Socket(uri.getHost(), uri.getPort());
            return RemoteBlobStore.open(socket);
        }
        throw new IllegalArgumentException("Unknown URL scheme '"
                + uri.getAuthority() + "'. Must be file:// or doss://");
    }
}
