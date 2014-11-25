package doss.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import doss.Blob;
import doss.core.Writables;

public abstract class ContainerTest {
    protected static final String TEST_DATA = "test\nstring\0a\r\nwith\tstrange\u2603characters";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    public Container container;

    @After
    public void tearDown() throws Exception {
        if (container != null) {
            container.close();
            container = null;
        }
    }

    @Test
    public void offsetsMustBeUnique() throws Exception {
        long offset1 = container.put(1, Writables.wrap("hello world"));
        long offset2 = container.put(2, Writables.wrap("how are you?"));
        assertNotEquals(offset1, offset2);
    }

    @Test
    public void readAndWrite() throws Exception {
        long offset = container.put(1, Writables.wrap(TEST_DATA));
        Blob blob = container.get(offset);
        assertEquals(1, blob.id());

        byte[] buf = new byte[(int) blob.size()];
        try (SeekableByteChannel channel = blob.openChannel()) {
            channel.read(ByteBuffer.wrap(buf));
        }

        assertEquals(TEST_DATA, new String(buf, "UTF-8"));
    }

}
