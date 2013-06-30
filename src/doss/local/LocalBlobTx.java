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
import doss.Writable;

public class LocalBlobTx implements BlobTx {
    
    final String id;
    final LocalBlobStore blobStore;
    final List<Long> addedBlobs = new ArrayList<Long>();
    
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
            public void writeTo(WritableByteChannel targetChannel) throws IOException {
                try (FileChannel sourceChannel = (FileChannel) Files.newByteChannel(source, StandardOpenOption.READ)) {
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
    public Blob put(Writable output) throws IOException {
        state.mustBeOpen();
        long blobId = blobStore.blobNumber.next();
        long offset = blobStore.container.put(Long.toString(blobId), output);
        blobStore.db.remember(blobId, offset);
        addedBlobs.add(blobId);
        return blobStore.container.get(offset);
    }

    /*
     * Events
     */
    
    void onPrepare() {}

    void onCommit() {
        onClose();
    }

    void onRollback() {
        for (Long blobId : addedBlobs) {
            blobStore.db.delete(blobId);
        }
        onClose();
    }

    void onClose() {
        blobStore.txs.remove(id());
    }

    /**
     * Transaction state machine
     */
    enum State {
        // transition table (@formatter:off)
        OPEN       { State close   (LocalBlobTx t) { return rollback(t); }
                     State prepare (LocalBlobTx t) { t.onPrepare();  return PREPARED; } 
                     State commit  (LocalBlobTx t) { return prepare(t).commit(t); } 
                     State rollback(LocalBlobTx t) { t.onRollback(); return ROLLEDBACK; }
                     State mustBeOpen()            { return this; }
                     State mustBeOpenOrPrepared()  { return this; }},
        PREPARED   { State commit  (LocalBlobTx t) { t.onCommit();   return COMMITTED; } 
                     State rollback(LocalBlobTx t) { t.onRollback(); return ROLLEDBACK; }
                     State mustBeOpenOrPrepared()  { return this; } },
        COMMITTED,
        ROLLEDBACK;
        
        // defaults
        State close   (LocalBlobTx t) { return this; }
        State prepare (LocalBlobTx t) { return mustBeOpen(); }
        State commit  (LocalBlobTx t) { return mustBeOpenOrPrepared(); }
        State rollback(LocalBlobTx t) { return mustBeOpenOrPrepared(); }
        State mustBeOpen()            { throw new IllegalStateException(name() + "; must be OPEN"); }
        State mustBeOpenOrPrepared()  { throw new IllegalStateException(name() + "; must be OPEN or PREPARED"); }
    }

    State state = State.OPEN;

    public void close()    { state = state.close   (this); }
    public void commit()   { state = state.commit  (this); }
    public void rollback() { state = state.rollback(this); }
    public void prepare()  { state = state.prepare (this); }

}
