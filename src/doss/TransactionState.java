package doss;

import java.io.IOException;

import doss.Transaction;

/**
 * Transaction state machine.
 * 
 * Defines valid state transitions for a {@link Transaction} and manages an
 * instance through those states. Passes all exceptions from the managed
 * transaction through to the caller.
 */
public enum TransactionState {

    OPEN {
        public TransactionState close(Transaction t)
                throws IllegalStateException, IOException {
            rollback(t);
            return assertPreparedCommittedOrRolledback();
        }

        public TransactionState prepare(Transaction t)
                throws IOException {
            t.prepare();
            return PREPARED;
        }

        public TransactionState commit(Transaction t) throws IOException {
            return prepare(t).commit(t);
        }

        public TransactionState rollback(Transaction t)
                throws IOException {
            t.rollback();
            return ROLLEDBACK;
        }

        TransactionState assertOpen() {
            return this;
        }

        TransactionState assertOpenOrPrepared() {
            return this;
        }
    },

    PREPARED {
        public TransactionState commit(Transaction t) throws IOException {
            t.commit();
            return COMMITTED;
        }

        public TransactionState rollback(Transaction t)
                throws IOException {
            t.rollback();
            return ROLLEDBACK;
        }

        TransactionState assertOpenOrPrepared() {
            return this;
        }
    },

    COMMITTED, ROLLEDBACK;

    // defaults
    public TransactionState close(Transaction t) throws IOException {
        t.close();
        return this;
    }

    public TransactionState prepare(Transaction t) throws IOException {
        return assertOpen();
    }

    public TransactionState commit(Transaction t) throws IOException {
        return assertOpenOrPrepared();
    }

    public TransactionState rollback(Transaction t) throws IOException {
        return assertOpenOrPrepared();
    }

    // helpers
    TransactionState assertOpen() {
        throw new IllegalStateException(name() + "; must be OPEN");
    }

    TransactionState assertOpenOrPrepared() {
        throw new IllegalStateException(name() + "; must be OPEN or PREPARED");
    }

    TransactionState assertPreparedCommittedOrRolledback() {
        throw new IllegalStateException(name()
                + "; must be PREPARED, COMMITTED or ROLLEDBACK");
    }
}