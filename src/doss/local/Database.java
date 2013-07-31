package doss.local;

import java.io.Closeable;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.sqlobject.mixins.GetHandle;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

abstract class Database implements Closeable, GetHandle {

    /**
     * Opens a DOSS database stored on the local filesystem.
     */
    public static Database open(Path dbPath) {
        return open(new DBI("jdbc:h2:file:" + dbPath + ";AUTO_SERVER=TRUE"));

    }

    public static Database open(DBI dbi) {
        return dbi.open(Database.class);
    }

    /**
     * Runs database migrations to populate or upgrade the schema.
     */
    public Database migrate() {
        // replace this with Flyway schema migrations (http://flywaydb.org/) as
        // soon as we need to start altering existing tables
        Schema schema = getHandle().attach(Schema.class);
        schema.createBlobsTable();
        schema.createIdSequence();
        return this;
    }

    interface Schema {
        @SqlUpdate("CREATE SEQUENCE IF NOT EXISTS ID_SEQ")
        void createIdSequence();

        @SqlUpdate("CREATE TABLE IF NOT EXISTS blobs (blob_id BIGINT PRIMARY KEY, container_id BIGINT, offset BIGINT)")
        void createBlobsTable();
    }

    @SqlQuery("SELECT NEXTVAL('ID_SEQ')")
    public abstract long nextId();

    @SqlUpdate("INSERT INTO blobs (blob_id, container_id, offset) VALUES (:blobId, :containerId, :offset)")
    public abstract void insertBlob(@Bind("blobId") long blobId,
            @Bind("containerId") long containerId, @Bind("offset") long offset);

    @SqlUpdate("DELETE FROM blobs WHERE blob_id = :blobId")
    public abstract void deleteBlob(@Bind("blobId") long blobId);

    @SqlQuery("SELECT container_id, offset FROM blobs WHERE blob_id = :blobId")
    @RegisterMapper(BlobLocationMapper.class)
    public abstract BlobLocation locateBlob(@Bind("blobId") long blobId);

    public static class BlobLocationMapper implements
            ResultSetMapper<BlobLocation> {
        @Override
        public BlobLocation map(int index, ResultSet r, StatementContext ctx)
                throws SQLException {
            return new BlobLocation(r.getLong("container_id"),
                    r.getLong("offset"));
        }
    }
}