package doss.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import doss.Blob;
import doss.Writable;

public abstract class ContainerTest {
    protected static final String TEST_ID = "weird\nid\0a\r\nwith\tstrange\u2601characters";
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
        long offset1 = container.put("a", stringOutput("hello world"));
        long offset2 = container.put("b", stringOutput("how are you?"));
        assertNotEquals(offset1, offset2);   
    }
    
    @Test
    public void readAndWrite() throws Exception {
        long offset = container.put(TEST_ID, stringOutput(TEST_DATA));
        Blob blob = container.get(offset);
        assertEquals(TEST_ID, blob.id());
        
        byte[] buf = new byte[(int) blob.size()];
        try (SeekableByteChannel channel = blob.openChannel()) {
            channel.read(ByteBuffer.wrap(buf));
        }
        
        assertEquals(TEST_DATA, new String(buf, "UTF-8"));    
    }

    protected Writable stringOutput(String s) {
        final byte[] bytes = s.getBytes();
        return new Writable() {
            public void writeTo(WritableByteChannel channel) throws IOException {
                channel.write(ByteBuffer.wrap(bytes));
            }

            @Override
            public long size() throws IOException {
                return bytes.length;
            }
        };
    }

}