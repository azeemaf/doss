package doss.local;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.attribute.FileTime;
import java.security.NoSuchAlgorithmException;

import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

import doss.Blob;

public class CachedMetadataBlob implements Blob {
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
}
