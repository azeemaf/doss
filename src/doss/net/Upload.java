package doss.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;

import doss.Blob;
import doss.BlobTx;
import doss.Writable;

/* XXX This is a hack just to get things working prior to redsign.  The BlobStore 
 * put interface is overly simplistic and keeps all state on the stack.  That doesn't
 * work so great with the stateful RPC methods.  For the time being we fork off a thread for
 * each upload and pass it chunks of data as they come in.
 */
public class Upload {
    private final Future<Blob> result;
    private final SynchronousQueue<ByteBuffer> queue = new SynchronousQueue<>();
    private final static ExecutorService pool = Executors.newCachedThreadPool();
    private long bytesWritten = 0;
    private final static ByteBuffer SENTINAL = ByteBuffer.allocate(1);

    public Upload(final BlobTx tx) {
        result = pool.submit(new Callable<Blob>() {
            @Override
            public Blob call() throws Exception {
                return tx.put(new Writable() {
                    @Override
                    public void writeTo(WritableByteChannel channel)
                            throws IOException {
                        try {
                            while (true) {
                                ByteBuffer b = queue.take();
                                if (b == SENTINAL)
                                    break;
                                while (b.remaining() > 0) {
                                    int written = channel.write(b);
                                    bytesWritten += written;
                                }
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
        });
    }

    public void write(ByteBuffer data) {
        try {
            queue.put(data);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public long finish() {
        try {
            try {
                queue.put(SENTINAL);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return result.get().id();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public long getBytesWritten() {
        return bytesWritten;
    }
}
