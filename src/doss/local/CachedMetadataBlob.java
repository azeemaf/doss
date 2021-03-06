package doss.local;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.attribute.FileTime;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

import doss.Blob;

/**
 * Wrapper around a Blob which caches metadata like digests in the SQL database.
 */
class CachedMetadataBlob implements Blob {
    final Database db;
    final Blob blob;

    CachedMetadataBlob(Database database, Blob blob) {
        this.db = database;
        this.blob = blob;
    }

    @Override
    public long id() {
        return blob.id();
    }

    @Override
    public InputStream openStream() throws IOException {
        return blob.openStream();
    }

    @Override
    public SeekableByteChannel openChannel() throws IOException {
        return blob.openChannel();
    }

    @Override
    public long size() throws IOException {
        // TODO: cache this (?)
        return blob.size();
    }

    @Override
    public FileTime created() throws IOException {
        // TODO: cache this (?)
        return blob.created();
    }

    @Override
    public String digest(String algorithm) throws NoSuchAlgorithmException, IOException {
        String canonAlgorithm = Digests.canonicalizeAlgorithm(algorithm);
        String digest = db.getDigest(id(), canonAlgorithm);
        if (digest == null) {
            digest = blob.digest(canonAlgorithm);
            try {
                db.insertDigest(id(), canonAlgorithm, digest);
            } catch (UnableToExecuteStatementException e) {
                // that's okay, another thread probably beat us to it
            }
        }
        return digest;
    }

    @Override
    public List<String> verify() throws IOException {
        List<String> errors = new ArrayList<>();
        errors.addAll(blob.verify());
        Map<String, String> digests = db.getDigests(id());
        for (Entry<String, String> entry : digests.entrySet()) {
            String algorithm = entry.getKey();
            String expected = entry.getValue();
            try {
                String actual = blob.digest(algorithm);
                if (!Objects.equals(expected, actual)) {
                    errors.add(algorithm + " digest mismatch: " + expected
                            + " cached in database " + actual + " on disk");
                }
            } catch (NoSuchAlgorithmException e) {
                errors.add("digest " + algorithm + ": " + e.getMessage());
            }
        }
        return errors;
    }
}
