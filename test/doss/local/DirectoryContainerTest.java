package doss.local;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import doss.Blob;
import doss.output.ChannelOutput;

public class DirectoryContainerTest {
    static final String TEST_ID = "weird\nid\0a\r\nwith\tstrange\u2601characters";
    static final String TEST_DATA = "test\nstring\0a\r\nwith\tstrange\u2603characters";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    
    public DirectoryContainer container;
    
    @Before
    public void setUp() throws Exception {
        container = new DirectoryContainer(folder.newFolder().toPath());
    }

    @Test
    public void readAndWrite() throws Exception {
        DirectoryContainer container = new DirectoryContainer(folder.newFolder().toPath());
        long offset = container.put(TEST_ID, stringOutput(TEST_DATA));
        Blob blob = container.get(offset);
        assertEquals(TEST_ID, blob.getId());
        assertEquals(TEST_DATA, blob.slurp());   
    }
    
    @Test
    public void offsetsMustBeUnique() throws Exception {
        DirectoryContainer container = new DirectoryContainer(folder.newFolder().toPath());
        long offset1 = container.put("a", stringOutput("hello world"));
        long offset2 = container.put("b", stringOutput("how are you?"));
        assertNotEquals(offset1, offset2);   
    }
    
    ChannelOutput stringOutput(final String s) {
        return new ChannelOutput() {
            public void writeTo(WritableByteChannel channel) throws IOException {
                channel.write(ByteBuffer.wrap(s.getBytes()));
            }
        };
    }

}
