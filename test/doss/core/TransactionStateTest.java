package doss.core;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;

import java.io.IOException;

import org.junit.Test;

import doss.core.TransactionState;

public class TransactionStateTest {

    // Open
    @Test
    public void testOpenPrepare() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(TransactionState.OPEN.prepare(t),
                TransactionState.PREPARED);
        verify(t).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testOpenCommit() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(TransactionState.OPEN.commit(t),
                TransactionState.COMMITTED);
        verify(t).prepare();
        verify(t).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testOpenRollback() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(TransactionState.OPEN.rollback(t),
                TransactionState.ROLLEDBACK);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t).rollback();
        verify(t, never()).close();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpenClose() throws IOException {
        Transaction t = mock(Transaction.class);
        TransactionState.OPEN.close(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    // Prepare
    @Test(expected = IllegalStateException.class)
    public void testPreparePrepare() throws IOException {
        Transaction t = mock(Transaction.class);
        TransactionState.PREPARED.prepare(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testPrepareCommit() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(TransactionState.PREPARED.commit(t),
                TransactionState.COMMITTED);
        verify(t, never()).prepare();
        verify(t).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testPrepareRollback() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(TransactionState.PREPARED.rollback(t),
                TransactionState.ROLLEDBACK);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testPrepareClose() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(TransactionState.PREPARED.close(t),
                TransactionState.PREPARED);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t).close();
    }

    // Commit
    @Test(expected = IllegalStateException.class)
    public void testCommitPrepare() throws IOException {
        Transaction t = mock(Transaction.class);
        TransactionState.COMMITTED.prepare(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test(expected = IllegalStateException.class)
    public void testCommitCommit() throws IOException {
        Transaction t = mock(Transaction.class);
        TransactionState.COMMITTED.commit(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();

    }

    @Test(expected = IllegalStateException.class)
    public void testCommitRollback() throws IOException {
        Transaction t = mock(Transaction.class);
        TransactionState.COMMITTED.rollback(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testCommitClose() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(TransactionState.COMMITTED.close(t),
                TransactionState.COMMITTED);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t).close();
    }

    // Rollback
    @Test(expected = IllegalStateException.class)
    public void testRolledbackPrepare() throws IOException {
        Transaction t = mock(Transaction.class);
        TransactionState.ROLLEDBACK.prepare(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test(expected = IllegalStateException.class)
    public void testRolledbackCommit() throws IOException {
        Transaction t = mock(Transaction.class);
        TransactionState.ROLLEDBACK.commit(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test(expected = IllegalStateException.class)
    public void testRolledbackRollback() throws IOException {
        Transaction t = mock(Transaction.class);
        TransactionState.ROLLEDBACK.rollback(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testRolledbackClose() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(TransactionState.ROLLEDBACK.close(t),
                TransactionState.ROLLEDBACK);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t).close();
    }
}
