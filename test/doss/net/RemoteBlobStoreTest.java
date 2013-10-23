package doss.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;

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
    BlobStore localStore;
    BlobStore remoteStore;
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final String s = "hello world";

    @Before
    public void setup() throws Exception {
        localStore = TempBlobStore.open();
        remoteStore = LoopbackBlobStore.open(localStore);
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
    }

    @Test(timeout = 5000)
    public void stressWriteAndRead() throws Exception {
        long id;
        try (BlobTx tx = remoteStore.begin()) {
            assertNotNull(tx);
            Blob b = tx.put(new Writable() {
                @Override
                public void writeTo(WritableByteChannel channel)
                        throws IOException {
                    channel.write(ByteBuffer.wrap(s.getBytes(UTF8)));
                    channel.write(ByteBuffer.wrap(s.getBytes(UTF8)));
                }
            });
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
    }
}
