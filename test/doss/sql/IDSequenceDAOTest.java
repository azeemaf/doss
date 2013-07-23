package doss.sql;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.PrintStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.logging.PrintStreamLog;
import org.skife.jdbi.v2.tweak.SQLLog;

public abstract class IDSequenceDAOTest {

    protected DBI dbi;

    @Before
    public void setup() {
        dbi = new DBI("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        SQLLog sqlLog = new PrintStreamLog(new PrintStream(System.out));
        dbi.setSQLLog(sqlLog);
        Tools.dropAllObjects(dbi);
        IDSequenceDAO dao = getDAO();
        dao.createSchema();
    }

    @After
    public void teardown() {
        Tools.dropAllObjects(dbi);
    }

    @Test
    public void testIncrement() {
        IDSequenceDAO dao = getDAO();
        Long first_id = dao.getNextId();
        Long second_id = dao.getNextId();
        assertNotEquals("Got different IDs", first_id, second_id);
        assertTrue("IDs increment", first_id < second_id);
    }

    abstract IDSequenceDAO getDAO();
}
