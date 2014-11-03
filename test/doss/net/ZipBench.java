package doss.net;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.compress.utils.CountingOutputStream;

import de.schlichtherle.truezip.rof.ReadOnlyFile;
import de.schlichtherle.truezip.zip.ZipEntry;
import de.schlichtherle.truezip.zip.ZipFile;
import de.schlichtherle.truezip.zip.ZipOutputStream;
import doss.Blob;
import doss.BlobStore;
import doss.BlobTx;
import doss.Writable;

public class ZipBench {

    public static void main(String[] args) throws Exception {
        int nBlobs = 10;
        int nEntries = 10;
        try (BlobStore bs = LoopbackBlobStore.open()) {
            List<Blob> blobs = createDummyZips(bs, nBlobs, nEntries);

            for (int i = 0; i < 50; i++) {
                long start = System.currentTimeMillis();
                for (Blob blob : blobs) {
                    try (ZipFile zip = new ZipFile(new ChannelReadOnlyFile(
                            blob.openChannel()))) {
                        for (int entry = nEntries - 1; entry >= 0; entry--) {
                            try (InputStream is = zip.getInputStream(Integer
                                    .toString(entry))) {
                                consume(is);
                            }
                        }
                    }
                }
                System.out.println("readback: "
                        + (System.currentTimeMillis() - start) + "ms");
            }
        }
    }

    private static List<Blob> createDummyZips(BlobStore bs, int nBlobs,
            int nEntries) throws IOException {
        long start = System.currentTimeMillis();
        List<Blob> blobs = new ArrayList<>();
        try (BlobTx tx = bs.begin()) {
            for (int i = 0; i < nBlobs; i++) {
                blobs.add(tx.put(dummyZip(nEntries)));
            }
            tx.commit();
        }
        System.out.println("creation: " + (System.currentTimeMillis() - start)
                + "ms");
        return blobs;
    }

    private static void consume(InputStream is) throws IOException {
        byte[] b = new byte[8192];
        while (is.read(b) >= 0) {
        }
    }

    public final static byte[] testData = new byte[4096];
    static {
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) i;
        }
    }

    private static Writable dummyZip(final int nEntries) {
        return new Writable() {
            @Override
            public long writeTo(WritableByteChannel channel) throws IOException {
                try (CountingOutputStream outc = new CountingOutputStream(
                        Channels.newOutputStream(channel))) {
                    try (ZipOutputStream out = new ZipOutputStream(
                            new BufferedOutputStream(outc, 8192))) {
                        for (int i = 0; i < nEntries; i++) {
                            out.putNextEntry(new ZipEntry(Integer.toString(i)));
                            out.write(testData);
                        }
                    }
                    return outc.getBytesWritten();
                }
            }
        };
    }

    /**
     * Adapter from SeekableByteChannel to ReadOnlyFile.
     */
    private static class ChannelReadOnlyFile implements ReadOnlyFile {
        private final SeekableByteChannel channel;

        public ChannelReadOnlyFile(SeekableByteChannel channel) {
            this.channel = channel;
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

        @Override
        public long getFilePointer() throws IOException {
            return channel.position();
        }

        @Override
        public long length() throws IOException {
            return channel.size();
        }

        @Override
        public int read() throws IOException {
            ByteBuffer b = ByteBuffer.allocate(1);
            channel.read(b);
            b.flip();
            return b.get();
        }

        @Override
        public int read(byte[] bytes) throws IOException {
            return channel.read(ByteBuffer.wrap(bytes));
        }

        @Override
        public int read(byte[] bytes, int offset, int length)
                throws IOException {
            return channel.read(ByteBuffer.wrap(bytes, offset, length));
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int offset, int length)
                throws IOException {
            int total = 0;
            while (total < bytes.length) {
                int n = read(bytes, offset + total, bytes.length - total);
                if (n < 0) {
                    throw new EOFException();
                }
                total += n;
            }
        }

        @Override
        public void seek(long position) throws IOException {
            channel.position(position);
        }
    }

}
