package doss;

import static org.junit.Assert.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import doss.core.Named;

public class IOTest extends DOSSTest {
    public void testChannelIO() throws Exception {
        long blobId;

        try (BlobTx tx = blobStore.begin()) {
            blobId = tx.put(TEST_BYTES).id();
            tx.commit();
        }

        byte[] buf = new byte[TEST_BYTES.length];
        try (SeekableByteChannel channel = blobStore.get(blobId).openChannel()) {
            channel.read(ByteBuffer.wrap(buf));
        }
        assertEquals(TEST_STRING, new String(buf, "UTF-8"));
    }

    public void testStreamIO() throws Exception {
        long id = writeTempBlob(blobStore, TEST_STRING).id();
        assertNotNull(id);

        Blob blob = blobStore.get(id);
        try (InputStream stream = blob.openStream()) {
            byte[] buffer = new byte[TEST_STRING.getBytes("UTF-8").length];
            stream.read(buffer);
            assertEquals(TEST_STRING, new String(buffer, "UTF-8"));
        }
    }

    public void testBytesIO() throws Exception {
        Named blob;

        try (BlobTx tx = blobStore.begin()) {
            blob = tx.put(TEST_BYTES);
            tx.commit();
        }
        
        assertNotNull(blob.id());
        assertEquals(TEST_STRING, slurp(blobStore.get(blob.id())));
    }
}
