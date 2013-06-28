package doss.local;

import static org.junit.Assert.*;

import org.junit.Test;

import doss.NoSuchBlobException;

public class MemoryBlobIndexTest {

    @Test
    public void testRemember() throws Exception {
        MemoryBlobIndex index = new MemoryBlobIndex();

        index.remember(1, 100);
        index.remember(2, 200);
        index.remember(3, 333);

        assertEquals(100, index.locate(1));
        assertEquals(200, index.locate(2));
        assertEquals(333, index.locate(3));
    }
    
    @Test(expected=NoSuchBlobException.class)
    public void testMissing() throws Exception {
        MemoryBlobIndex index = new MemoryBlobIndex();

        index.remember(1, 100);
        index.remember(2, 200);
        
        index.locate(4);
    }

    @Test(expected=NoSuchBlobException.class)
    public void testDeleted() throws Exception {
        MemoryBlobIndex index = new MemoryBlobIndex();

        index.remember(1, 100);
        index.delete(1);
        index.locate(1);
    }
    
}
