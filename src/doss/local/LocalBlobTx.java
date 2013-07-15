package doss.local;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import doss.Blob;
import doss.BlobTx;
import doss.Transaction;
import doss.TransactionStateMachine;
import doss.Writable;

public class LocalBlobTx implements BlobTx {

    final String id;
    final LocalBlobStore blobStore;
    final List<Long> addedBlobs = new ArrayList<Long>();

    private TransactionStateMachine state = TransactionStateMachine.OPEN;

    public LocalBlobTx(String id, LocalBlobStore blobStore) {
        this.id = id;
        this.blobStore = blobStore;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public Blob put(final Path source) throws IOException {
        return put(new Writable() {
            public void writeTo(WritableByteChannel targetChannel)
                    throws IOException {
                try (FileChannel sourceChannel = (FileChannel) Files
                        .newByteChannel(source, StandardOpenOption.READ)) {
                    sourceChannel.transferTo(0, Long.MAX_VALUE, targetChannel);
                }
            }

            public long size() throws IOException {
                return Files.size(source);
            }
        });
    }

    @Override
    public Blob put(String contents) throws IOException {
        return put(contents.getBytes(Charset.forName("UTF-8")));
    }

    @Override
    public Blob put(final byte[] bytes) throws IOException {
        return put(new Writable() {
            public void writeTo(WritableByteChannel channel) throws IOException {
                channel.write(ByteBuffer.wrap(bytes));
            }

            public long size() {
                return bytes.length;
            }
        });
    }

    @Override
    public synchronized Blob put(Writable output) throws IOException {
        if (state != TransactionStateMachine.OPEN) {
            throw new IllegalStateException(
                    "can only put() to an open transaction");
        }
        long blobId = blobStore.blobNumber.next();
        long offset = blobStore.container.put(Long.toString(blobId), output);
        blobStore.db.remember(blobId, offset);
        addedBlobs.add(blobId);
        return blobStore.container.get(offset);
    }

    // This private transaction will be controlled by the
    // TransactionStateMachine. As the TransactionStateMachine moves this
    // private transaction through it's states, the private transaction will
    // call back into the LocalBlobTx instance to manage data and resources.
    // This allows us to have transaction state transition logic controlled
    // separately to the central data management concerns of this class.
    Transaction controlledTransaction = new Transaction() {

        @Override
        public void commit() throws IOException {
            close();
        }

        @Override
        public void rollback() throws IOException {
            for (Long blobId : addedBlobs) {
                blobStore.db.delete(blobId);
            }
            close();
        }

        @Override
        public void prepare() {
            // TODO Auto-generated method stub

        }

        @Override
        public void close() throws IllegalStateException {
            blobStore.txs.remove(id());
        }

    };

    // Transaction interface.
    // Pass all transaction calls to the TransactionStateMachine and have it
    // call back into out private controlled transaction above.
    public synchronized void close() throws IllegalStateException, IOException {
        state = state.close(controlledTransaction);
    }

    public synchronized void commit() throws IllegalStateException, IOException {
        state = state.commit(controlledTransaction);
    }

    public synchronized void rollback() throws IllegalStateException, IOException {
        state = state.rollback(controlledTransaction);
    }

    public synchronized void prepare() throws IllegalStateException, IOException {
        state = state.prepare(controlledTransaction);
    }

}
