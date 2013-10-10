package doss.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import doss.Blob;
import doss.BlobStore;
import doss.DOSSTest;
import doss.local.LocalBlobStore;

public class RemoteBlobStoreTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    BlobStore localStore;
    BlobStoreServer server;
    RemoteBlobStore remoteStore;
    Thread serverThread;
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final String s = "hello world";

    @Before
    public void setup() throws Exception {
        Path root = folder.getRoot().toPath();
        LocalBlobStore.init(root);
        localStore = LocalBlobStore.open(root);
        server = new BlobStoreServer(localStore);
        serverThread = new Thread(server);
        serverThread.start();
        Thread.sleep(100);
        remoteStore = new RemoteBlobStore(new Socket(
                "localhost", 1234));
    }

    @After
    public void tearDown() {
        remoteStore.close();
        server.close();
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
    public void testBegin() throws Exception {
        assertNotNull(remoteStore.begin());
    }

}
