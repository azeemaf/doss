package doss.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import doss.Blob;
import doss.BlobStore;
import doss.BlobTx;
import doss.DOSSTest;
import doss.Writable;
import doss.local.TempBlobStore;

public class RemoteBlobStoreTest {
    TempBlobStore localStore;
    BlobStore remoteStore;
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final String s = "hello world";

    @Before
    public void setup() throws Exception {
        localStore = TempBlobStore.open();
        remoteStore = SecureLoopbackBlobStore.open(localStore);
    }

    @After
    public void tearDown() {
        remoteStore.close();
        localStore.close();
    }

    @Test(timeout = 1000)
    public void testStat() throws Exception {
        long id = DOSSTest.writeTempBlob(localStore, s).id();
        Blob blob = remoteStore.get(id);
        assertNotNull(blob);
        assertEquals(id, blob.id());
        assertEquals(s.getBytes(UTF8).length, blob.size());
        assertNotNull(blob.created());
    }

    @Test(timeout = 1000)
    public void testStatLegacy() throws Exception {
        Path tmp = Files.createTempFile("doss-test", ".tmp");
        try {
            Files.write(tmp, s.getBytes(UTF8));
            Blob blob = remoteStore.getLegacy(tmp);
            assertNotNull(blob);
            assertEquals(s.getBytes(UTF8).length, blob.size());
            assertNotNull(blob.created());
            Blob blob2 = remoteStore.getLegacy(tmp);
            assertEquals(blob.id(), blob2.id());
            ByteBuffer b = ByteBuffer.allocate(s.getBytes(UTF8).length);
            try (SeekableByteChannel channel = blob.openChannel()) {
                channel.read(b);
            }
            assertEquals(s, new String(b.array(), UTF8));
        } finally {
            Files.delete(tmp);
        }
    }

    @Test(timeout = 1000)
    public void testRead() throws Exception {
        long id = DOSSTest.writeTempBlob(localStore, s).id();
        Blob blob = remoteStore.get(id);
        assertNotNull(blob);
        ByteBuffer b = ByteBuffer.allocate(s.getBytes(UTF8).length);
        try (SeekableByteChannel channel = blob.openChannel()) {
            channel.read(b);
        }
        assertEquals(s, new String(b.array(), UTF8));
    }

    @Test(timeout = 1000)
    public void testWriteAndRead() throws Exception {
        long id;
        try (BlobTx tx = remoteStore.begin()) {
            assertNotNull(tx);
            Blob b = tx.put(s.getBytes(UTF8));
            assertNotNull(b);
            id = b.id();
            tx.commit();
        }
        Blob blob = remoteStore.get(id);
        assertNotNull(blob);
        ByteBuffer b = ByteBuffer.allocate(s.getBytes(UTF8).length);
        try (SeekableByteChannel channel = blob.openChannel()) {
            channel.read(b);
        }
        assertEquals(s, new String(b.array(), UTF8));
        assertEquals("5eb63bbbe01eeed093cb22bb8f5acdc3", blob.digest("md5"));
    }

    @Test(timeout = 5000)
    public void stressWriteAndRead() throws Exception {
        long id;
        final byte[] bytes = s.getBytes();
        try (BlobTx tx = remoteStore.begin()) {
            assertNotNull(tx);
            Blob b = tx.put(new Writable() {
                @Override
                public void writeTo(WritableByteChannel channel)
                        throws IOException {
                    long n = channel.write(ByteBuffer.wrap(bytes));
                    n += channel.write(ByteBuffer.wrap(bytes));
                }
            });
            assertNotNull(b);
            id = b.id();
            tx.commit();
        }
        Blob blob = remoteStore.get(id);
        assertNotNull(blob);
        ByteBuffer b = ByteBuffer.allocate(bytes.length * 2);
        try (SeekableByteChannel channel = blob.openChannel()) {
            assertEquals(bytes.length * 2, channel.read(b));
        }
        assertEquals(s + s, new String(b.array(), UTF8));
    }
}
