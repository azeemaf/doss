package doss.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.PrintStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.logging.PrintStreamLog;
import org.skife.jdbi.v2.tweak.SQLLog;

import doss.NoSuchBlobException;

public class SqlBlobIndexTest {

    private DBI dbi;

    @Before
    public void setup() {
        dbi = new DBI("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        SQLLog sqlLog = new PrintStreamLog(new PrintStream(System.out));
        dbi.setSQLLog(sqlLog);
        killemall();

        // DDL is going to commit the transaction in a h2 db no matter what,
        // FYI (unless you use TEMPORARY, which would mean having a schema
        // definition specific to the tests).
        BlobIndexSchemaDAO schema = dbi.onDemand(BlobIndexSchemaDAO.class);
        schema.createSchema();
    }

    @After
    public void teardown() {
        killemall();
    }

    private void killemall() {
        Handle h = null;
        try {
            h = dbi.open();
            h.execute("DROP ALL OBJECTS");
        } finally {
            if (h != null) {
                h.close();
            }
        }
    }

    @Test
    public void testRemember() {

        BlobIndex index = new SqlBlobIndex(dbi);

        long firstBlob = 0L;
        long secondBlob = 1L;
        long thirdBlob = 2L;
        long sharedPosition = 0L;
        long differentPosition = 1L;

        index.remember(firstBlob, sharedPosition);
        index.remember(secondBlob, sharedPosition);
        index.remember(thirdBlob, differentPosition);

        assertEquals("First blob position is OK", sharedPosition,
                index.locate(firstBlob));

        assertEquals("Second blob position is OK", sharedPosition,
                index.locate(secondBlob));

        assertEquals("Third blob position is OK", sharedPosition,
                index.locate(secondBlob));
    }

    @Test
    public void testDelete() {
        BlobIndex index = new SqlBlobIndex(dbi);

        long firstBlob = 0L;
        long secondBlob = 1L;
        long sharedPosition = 0L;

        index.remember(firstBlob, sharedPosition);
        index.remember(secondBlob, sharedPosition);

        assertEquals("First blob position is OK", sharedPosition,
                index.locate(firstBlob));

        assertEquals("Second blob position is OK", sharedPosition,
                index.locate(secondBlob));

        index.delete(firstBlob);
        Boolean firstBlobDeleted = false;
        try {
            index.locate(firstBlob);
        } catch (NoSuchBlobException e) {
            firstBlobDeleted = true;
        }
        assertTrue("First blob got deleted", firstBlobDeleted);

        assertEquals("Second blob is OK after first blob deleted",
                sharedPosition, index.locate(secondBlob));

        index.delete(secondBlob);
        Boolean secondBlobDeleted = false;
        try {
            index.locate(secondBlob);
        } catch (NoSuchBlobException e) {
            secondBlobDeleted = true;
        }
        assertTrue("Second blob got deleted", secondBlobDeleted);
    }

    @Test
    public void testMaxValue() {
        BlobIndex index = new SqlBlobIndex(dbi);

        long firstBlob = Long.MAX_VALUE;
        long firstBlobContainerOffset = Long.MAX_VALUE;

        index.remember(firstBlob, firstBlobContainerOffset);

        assertEquals("First blob position is OK", firstBlobContainerOffset,
                index.locate(firstBlob));
    }

    @Test
    public void testMinValue() {
        BlobIndex index = new SqlBlobIndex(dbi);

        long firstBlob = Long.MIN_VALUE;
        long firstBlobContainerOffset = Long.MIN_VALUE;

        index.remember(firstBlob, firstBlobContainerOffset);

        assertEquals("First blob position is OK", firstBlobContainerOffset,
                index.locate(firstBlob));
    }

    @Test
    public void testHandleOutsideTransaction() {

        Handle h = null;
        try {
            h = dbi.open();
            BlobIndex index = new SqlBlobIndex(h);

            long firstBlob = 0;
            long firstBlobContainerOffset = 0;
            index.remember(firstBlob, firstBlobContainerOffset);

            assertEquals("First blob position is OK", firstBlobContainerOffset,
                    index.locate(firstBlob));

        } finally {
            if (h != null) {
                h.close();
            }
        }

    }

    @Test
    public void testTransactionParticipation() {

        final BlobIndex index = new SqlBlobIndex(dbi);

        final long firstBlob = 0L;
        long secondBlob = 1L;

        index.remember(firstBlob, 0);
        index.remember(secondBlob, 0);

        assertEquals("First blob position is OK", 0, index.locate(firstBlob));

        Boolean rolledBack = false;
        try {
            dbi.inTransaction(new TransactionCallback<Void>() {

                @Override
                public Void inTransaction(Handle conn, TransactionStatus status)
                        throws Exception {

                    final BlobIndex totallyDifferentIndexInstance = new SqlBlobIndex(
                            conn);
                    totallyDifferentIndexInstance.remember(firstBlob, 1);

                    assertEquals(
                            "First blob position is OK after update in transaction",
                            1, totallyDifferentIndexInstance.locate(firstBlob));

                    assertEquals(
                            "First blob position is OK after update /outside/ transaction",
                            0, index.locate(firstBlob));

                    throw new RuntimeException("rollback, please");
                }

            });
        } catch (CallbackFailedException e) {
            rolledBack = true;
        }
        assertTrue("Rollback happened", rolledBack);

        assertEquals("First blob position is OK after transaction rolled back",
                0, index.locate(firstBlob));
    }
}
