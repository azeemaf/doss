package doss.db;

import static org.junit.Assert.*;

import org.junit.Test;

public class MemoryDbTest {

    @Test
    public void testRemember() throws Exception {
        MemoryDb db = new MemoryDb();

        db.remember(1, 100);
        db.remember(2, 200);
        db.remember(3, 333);

        assertEquals(100, db.locate(1));
        assertEquals(200, db.locate(2));
        assertEquals(333, db.locate(3));
    }
    
    @Test(expected=NoSuchBlobException.class)
    public void testMissing() throws Exception {
        MemoryDb db = new MemoryDb();

        db.remember(1, 100);
        db.remember(2, 200);
        
        db.locate(4);
    }

    @Test(expected=NoSuchBlobException.class)
    public void testDeleted() throws Exception {
        MemoryDb db = new MemoryDb();

        db.remember(1, 100);
        db.delete(1);
        db.locate(1);
    }
    
}
