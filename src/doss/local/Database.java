package doss.local;

import java.io.Closeable;
import java.nio.file.Path;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Transaction;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.sqlobject.mixins.GetHandle;
import org.skife.jdbi.v2.sqlobject.mixins.Transactional;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import com.googlecode.flyway.core.Flyway;

abstract class Database implements Closeable, GetHandle, Transactional<Database> {

    /*
     * Connection meta data URL doesn't include H2 switches. We have to manually
     * include them for flyway migrations
     */
    static final String H2_SWITCHES = ";AUTO_SERVER=true;MVCC=true";

    /**
     * Opens an in-memory database for internal testing.
     */
    static Database open() {
        return open(new DBI("jdbc:h2:mem:testing;MVCC=true"));
    }

    @Override
    public abstract void close();

    /**
     * Opens a DOSS database stored on the local filesystem.
     */
    public static Database open(Path dbPath) {
        return open(new DBI("jdbc:h2:file:" + dbPath + "/doss" + H2_SWITCHES));
    }

    public static Database open(DBI dbi) {
        return dbi.open(Database.class);
    }

    /**
     * Runs database migrations to populate or upgrade the schema.
     */
    public Database migrate() {
        try {
            // silence flyway's annoying default logging
            Logger.getLogger("com.googlecode.flyway").setLevel(Level.SEVERE);
            DatabaseMetaData md = getHandle().getConnection().getMetaData();
            Flyway flyway = new Flyway();
            flyway.setDataSource(md.getURL() + H2_SWITCHES, md.getUserName(), "");
            flyway.setLocations("doss/migrations");
            flyway.setInitOnMigrate(true);
            flyway.migrate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    @SqlQuery("SELECT NEXTVAL('ID_SEQ')")
    public abstract long nextId();

    @SqlUpdate("INSERT INTO blobs (blob_id, container_id, offset) VALUES (:blobId, :containerId, :offset)")
    public abstract void insertBlob(@Bind("blobId") long blobId, @Bind("containerId") long containerId, @Bind("offset") long offset);

    @SqlUpdate("DELETE FROM blobs WHERE blob_id = :blobId")
    public abstract void deleteBlob(@Bind("blobId") long blobId);

    @SqlQuery("SELECT container_id, offset FROM blobs WHERE blob_id = :blobId")
    @RegisterMapper(BlobLocationMapper.class)
    public abstract BlobLocation locateBlob(@Bind("blobId") long blobId);

    public static class BlobLocationMapper implements ResultSetMapper<BlobLocation> {
        @Override
        public BlobLocation map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            return new BlobLocation(r.getLong("container_id"), r.getLong("offset"));
        }
    }

    @SqlQuery("SELECT container_id FROM containers WHERE sealed = 0 AND AREA = :area")
    public abstract Long findAnOpenContainer(@Bind("area") String area);

    @SqlUpdate("INSERT INTO containers (area) VALUES (:area)")
    @GetGeneratedKeys
    public abstract long createContainer(@Bind("area") String name);

    @SqlUpdate("UPDATE containers SET sealed = true WHERE container_id = :id")
    public abstract long sealContainer(@Bind("id") long containerId);

    @SqlQuery("SELECT blob_id FROM legacy_paths WHERE legacy_path = :legacy_path FOR UPDATE")
    public abstract Long findBlobIdForLegacyPathAndLock(@Bind("legacy_path") String legacyPath);

    @SqlUpdate("INSERT INTO legacy_paths (blob_id, legacy_path) VALUES (:blob_id, :legacy_path)")
    public abstract long insertLegacy(@Bind("blob_id") long blobId, @Bind("legacy_path") String legacyPath);

    @Transaction
    public Long findOrInsertBlobIdByLegacyPath(String legacyPath) {
        Long blobId = findBlobIdForLegacyPathAndLock(legacyPath);
        if (blobId == null) {
            blobId = nextId();
            insertLegacy(blobId, legacyPath);
        }
        return blobId;
    }

    @SqlQuery("SELECT legacy_path FROM legacy_paths WHERE blob_id = :blob_id")
    public abstract String locateLegacy(@Bind("blob_id") long blobId);
}
