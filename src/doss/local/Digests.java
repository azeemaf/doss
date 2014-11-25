package doss.local;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.DatatypeConverter;

import doss.Blob;

class Digests {
    public static String canonicalizeAlgorithm(String algorithm) {
        return algorithm.replace("-", "").toLowerCase();
    }

    public static String calculate(String algorithm, ReadableByteChannel channel)
            throws NoSuchAlgorithmException,
            IOException {
        MessageDigest md = MessageDigest.getInstance(canonicalizeAlgorithm(algorithm));
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        while (channel.read(buffer) > 0) {
            buffer.flip();
            md.update(buffer);
            buffer.clear();
        }
        return DatatypeConverter.printHexBinary(md.digest()).toLowerCase();
    }

    public static String calculate(String algorithm, Blob blob) throws NoSuchAlgorithmException,
    IOException {
        try (ReadableByteChannel chan = blob.openChannel()) {
            return calculate(algorithm, chan);
        }
    }

}
