package doss;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;

import java.io.IOException;

import org.junit.Test;

public class TransactionStateMachineTest {

    // Open
    @Test
    public void testOpenPrepare() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(TransactionStateMachine.OPEN.prepare(t),
                TransactionStateMachine.PREPARED);
        verify(t).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testOpenCommit() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(TransactionStateMachine.OPEN.commit(t),
                TransactionStateMachine.COMMITTED);
        verify(t).prepare();
        verify(t).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testOpenRollback() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(TransactionStateMachine.OPEN.rollback(t),
                TransactionStateMachine.ROLLEDBACK);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t).rollback();
        verify(t, never()).close();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpenClose() throws IOException {
        Transaction t = mock(Transaction.class);
        TransactionStateMachine.OPEN.close(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    // Prepare
    @Test(expected = IllegalStateException.class)
    public void testPreparePrepare() throws IOException {
        Transaction t = mock(Transaction.class);
        TransactionStateMachine.PREPARED.prepare(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testPrepareCommit() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(TransactionStateMachine.PREPARED.commit(t),
                TransactionStateMachine.COMMITTED);
        verify(t, never()).prepare();
        verify(t).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testPrepareRollback() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(TransactionStateMachine.PREPARED.rollback(t),
                TransactionStateMachine.ROLLEDBACK);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testPrepareClose() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(TransactionStateMachine.PREPARED.close(t),
                TransactionStateMachine.PREPARED);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t).close();
    }

    // Commit
    @Test(expected = IllegalStateException.class)
    public void testCommitPrepare() throws IOException {
        Transaction t = mock(Transaction.class);
        TransactionStateMachine.COMMITTED.prepare(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test(expected = IllegalStateException.class)
    public void testCommitCommit() throws IOException {
        Transaction t = mock(Transaction.class);
        TransactionStateMachine.COMMITTED.commit(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();

    }

    @Test(expected = IllegalStateException.class)
    public void testCommitRollback() throws IOException {
        Transaction t = mock(Transaction.class);
        TransactionStateMachine.COMMITTED.rollback(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testCommitClose() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(TransactionStateMachine.COMMITTED.close(t),
                TransactionStateMachine.COMMITTED);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t).close();
    }

    // Rollback
    @Test(expected = IllegalStateException.class)
    public void testRolledbackPrepare() throws IOException {
        Transaction t = mock(Transaction.class);
        TransactionStateMachine.ROLLEDBACK.prepare(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test(expected = IllegalStateException.class)
    public void testRolledbackCommit() throws IOException {
        Transaction t = mock(Transaction.class);
        TransactionStateMachine.ROLLEDBACK.commit(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test(expected = IllegalStateException.class)
    public void testRolledbackRollback() throws IOException {
        Transaction t = mock(Transaction.class);
        TransactionStateMachine.ROLLEDBACK.rollback(t);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t, never()).close();
    }

    @Test
    public void testRolledbackClose() throws IOException {
        Transaction t = mock(Transaction.class);
        assertEquals(TransactionStateMachine.ROLLEDBACK.close(t),
                TransactionStateMachine.ROLLEDBACK);
        verify(t, never()).prepare();
        verify(t, never()).commit();
        verify(t, never()).rollback();
        verify(t).close();
    }
}
