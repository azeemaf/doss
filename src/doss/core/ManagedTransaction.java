package doss.core;

import java.io.IOException;

/**
 * A transaction with a managed lifecycle.
 */
public abstract class ManagedTransaction implements Transaction {
    
    protected State state = State.OPEN;
    
    abstract protected Transaction getCallbacks();

    public synchronized void close() throws IllegalStateException, IOException {
        state = state.close(getCallbacks());
    }

    public synchronized void commit() throws IllegalStateException, IOException {
        state = state.commit(getCallbacks());
    }

    public synchronized void rollback() throws IllegalStateException, IOException {
        state = state.rollback(getCallbacks());
    }

    public synchronized void prepare() throws IllegalStateException, IOException {
        state = state.prepare(getCallbacks());
    }
    
    /**
     * Transaction state machine.
     * 
     * Defines valid state transitions for a {@link Transaction} and manages an
     * instance through those states. Passes all exceptions from the managed
     * transaction through to the caller.
     */
    protected static enum State {

        OPEN {
            public State close(Transaction t)
                    throws IllegalStateException, IOException {
                rollback(t);
                return assertPreparedCommittedOrRolledback();
            }

            public State prepare(Transaction t)
                    throws IOException {
                t.prepare();
                return PREPARED;
            }

            public State commit(Transaction t) throws IOException {
                return prepare(t).commit(t);
            }

            public State rollback(Transaction t)
                    throws IOException {
                t.rollback();
                return ROLLEDBACK;
            }

            public State assertOpen() {
                return this;
            }

            public State assertOpenOrPrepared() {
                return this;
            }
        },

        PREPARED {
            public State commit(Transaction t) throws IOException {
                t.commit();
                return COMMITTED;
            }

            public State rollback(Transaction t)
                    throws IOException {
                t.rollback();
                return ROLLEDBACK;
            }

            public State assertOpenOrPrepared() {
                return this;
            }
        },

        COMMITTED, ROLLEDBACK;

        // defaults
        public State close(Transaction t) throws IOException {
            t.close();
            return this;
        }

        public State prepare(Transaction t) throws IOException {
            return assertOpen();
        }

        public State commit(Transaction t) throws IOException {
            return assertOpenOrPrepared();
        }

        public State rollback(Transaction t) throws IOException {
            return assertOpenOrPrepared();
        }

        // helpers
        public State assertOpen() {
            throw new IllegalStateException(name() + "; must be OPEN");
        }

        public State assertOpenOrPrepared() {
            throw new IllegalStateException(name() + "; must be OPEN or PREPARED");
        }

        public State assertPreparedCommittedOrRolledback() {
            throw new IllegalStateException(name()
                    + "; must be PREPARED, COMMITTED or ROLLEDBACK");
        }
    }
}