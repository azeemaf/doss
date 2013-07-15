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
public enum TransactionStateMachine {

    OPEN {
        public TransactionStateMachine close(Transaction t)
                throws IllegalStateException, IOException {
            rollback(t);
            return assertPreparedCommittedOrRolledback();
        }

        public TransactionStateMachine prepare(Transaction t)
                throws IOException {
            t.prepare();
            return PREPARED;
        }

        public TransactionStateMachine commit(Transaction t) throws IOException {
            return prepare(t).commit(t);
        }

        public TransactionStateMachine rollback(Transaction t)
                throws IOException {
            t.rollback();
            return ROLLEDBACK;
        }

        TransactionStateMachine assertOpen() {
            return this;
        }

        TransactionStateMachine assertOpenOrPrepared() {
            return this;
        }
    },

    PREPARED {
        public TransactionStateMachine commit(Transaction t) throws IOException {
            t.commit();
            return COMMITTED;
        }

        public TransactionStateMachine rollback(Transaction t)
                throws IOException {
            t.rollback();
            return ROLLEDBACK;
        }

        TransactionStateMachine assertOpenOrPrepared() {
            return this;
        }
    },

    COMMITTED, ROLLEDBACK;

    // defaults
    public TransactionStateMachine close(Transaction t) throws IOException {
        t.close();
        return this;
    }

    public TransactionStateMachine prepare(Transaction t) throws IOException {
        return assertOpen();
    }

    public TransactionStateMachine commit(Transaction t) throws IOException {
        return assertOpenOrPrepared();
    }

    public TransactionStateMachine rollback(Transaction t) throws IOException {
        return assertOpenOrPrepared();
    }

    // helpers
    TransactionStateMachine assertOpen() {
        throw new IllegalStateException(name() + "; must be OPEN");
    }

    TransactionStateMachine assertOpenOrPrepared() {
        throw new IllegalStateException(name() + "; must be OPEN or PREPARED");
    }

    TransactionStateMachine assertPreparedCommittedOrRolledback() {
        throw new IllegalStateException(name()
                + "; must be PREPARED, COMMITTED or ROLLEDBACK");
    }
}