package doss.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.Socket;

import org.junit.Test;

import doss.Blob;
import doss.BlobStore;

public class RemoteBlobStoreTest {
    @Test(timeout = 1000)
    public void test() throws IOException {
        Blob mockBlob = mock(Blob.class);
        BlobStore mockStore = mock(BlobStore.class);
        when(mockStore.get(0)).thenReturn(mockBlob);
        when(mockBlob.size()).thenReturn(1234L);

        try (BlobStoreServer server = new BlobStoreServer(mockStore)) {
            new Thread(server).start();
            try (RemoteBlobStore blobStore = new RemoteBlobStore(new Socket(
                    "localhost", 1234))) {
                Blob blob = blobStore.get(0);
                assertNotNull(blob);
                assertEquals(1234L, blobStore.get(0).size());
            }
        }
    }
}
