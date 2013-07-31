package doss.core;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.junit.Test;

public class ManagedTransactionTest {

    // Open
    @Test
    public void testOpenPrepare() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(ManagedTransaction.State.OPEN.prepare(t),
                ManagedTransaction.State.PREPARED);
        verify(t).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testOpenCommit() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(ManagedTransaction.State.OPEN.commit(t),
                ManagedTransaction.State.COMMITTED);
        verify(t).prepare();
        verify(t).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testOpenRollback() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(ManagedTransaction.State.OPEN.rollback(t),
                ManagedTransaction.State.ROLLEDBACK);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t).rollback();
        verify(t, never()).close();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpenClose() throws IOException {
        Transaction t = mock(Transaction.class);
        ManagedTransaction.State.OPEN.close(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    // Prepare
    @Test(expected = IllegalStateException.class)
    public void testPreparePrepare() throws IOException {
        Transaction t = mock(Transaction.class);
        ManagedTransaction.State.PREPARED.prepare(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testPrepareCommit() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(ManagedTransaction.State.PREPARED.commit(t),
                ManagedTransaction.State.COMMITTED);
        verify(t, never()).prepare();
        verify(t).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testPrepareRollback() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(ManagedTransaction.State.PREPARED.rollback(t),
                ManagedTransaction.State.ROLLEDBACK);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testPrepareClose() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(ManagedTransaction.State.PREPARED.close(t),
                ManagedTransaction.State.PREPARED);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t).close();
    }

    // Commit
    @Test(expected = IllegalStateException.class)
    public void testCommitPrepare() throws IOException {
        Transaction t = mock(Transaction.class);
        ManagedTransaction.State.COMMITTED.prepare(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test(expected = IllegalStateException.class)
    public void testCommitCommit() throws IOException {
        Transaction t = mock(Transaction.class);
        ManagedTransaction.State.COMMITTED.commit(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();

    }

    @Test(expected = IllegalStateException.class)
    public void testCommitRollback() throws IOException {
        Transaction t = mock(Transaction.class);
        ManagedTransaction.State.COMMITTED.rollback(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testCommitClose() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(ManagedTransaction.State.COMMITTED.close(t),
                ManagedTransaction.State.COMMITTED);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t).close();
    }

    // Rollback
    @Test(expected = IllegalStateException.class)
    public void testRolledbackPrepare() throws IOException {
        Transaction t = mock(Transaction.class);
        ManagedTransaction.State.ROLLEDBACK.prepare(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test(expected = IllegalStateException.class)
    public void testRolledbackCommit() throws IOException {
        Transaction t = mock(Transaction.class);
        ManagedTransaction.State.ROLLEDBACK.commit(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test(expected = IllegalStateException.class)
    public void testRolledbackRollback() throws IOException {
        Transaction t = mock(Transaction.class);
        ManagedTransaction.State.ROLLEDBACK.rollback(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testRolledbackClose() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(ManagedTransaction.State.ROLLEDBACK.close(t),
                ManagedTransaction.State.ROLLEDBACK);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t).close();
    }
}
