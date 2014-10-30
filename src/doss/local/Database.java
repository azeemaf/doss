package doss.local;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.compress.utils.Charsets;
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

abstract class Database implements Closeable, GetHandle,
        Transactional<Database> {

    /*
     * Connection meta data URL doesn't include H2 switches. We have to manually
     * include them for flyway migrations
     */
    static final String H2_SWITCHES = ";AUTO_SERVER=true;MVCC=true";

    private String jdbcUrl;

    /**
     * Opens an in-memory database for internal testing.
     */
    static Database open() {
        return open(new DBI("jdbc:h2:mem:testing;MVCC=true"));
    }

    public static Database open(String jdbcUrl) {
        Database db = open(new DBI(jdbcUrl));
        db.jdbcUrl = jdbcUrl;
        return db;
    }

    @Override
    public abstract void close();

    /**
     * Opens a DOSS database stored on the local filesystem.
     */
    public static Database open(Path dbPath) {
        Path urlFile = dbPath.resolve("jdbc-url");
        if (Files.exists(dbPath.resolve("jdbc-url"))) {
            try {
                return open(Files.readAllLines(urlFile, Charsets.UTF_8).get(0));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return open("jdbc:h2:file:" + dbPath + "/doss" + H2_SWITCHES);
    }

    public static Database open(DBI dbi) {
        return dbi.open(Database.class);
    }

    /**
     * Runs database migrations to populate or upgrade the schema.
     */
    public Database migrate() {
        try {
            Flyway flyway = openFlyway();
            flyway.setInitOnMigrate(true);
            flyway.migrate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    private Flyway openFlyway() throws SQLException {
        // silence flyway's annoying default logging
        Logger.getLogger("com.googlecode.flyway").setLevel(Level.SEVERE);
        DatabaseMetaData md = getHandle().getConnection().getMetaData();
        Flyway flyway = new Flyway();
        flyway.setDataSource(jdbcUrl != null ? jdbcUrl : md.getURL()
                + H2_SWITCHES, md.getUserName(), "");
        flyway.setLocations("doss/migrations");
        return flyway;
    }

    public String version() {
        try {
            Flyway flyway = openFlyway();
            return flyway.info().current().getVersion().toString();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @SqlQuery("SELECT NEXTVAL('ID_SEQ')")
    public abstract long nextId();

    @SqlUpdate("INSERT INTO blobs (blob_id, container_id, offset) VALUES (:blobId, :containerId, :offset)")
    public abstract void insertBlob(@Bind("blobId") long blobId,
            @Bind("containerId") long containerId,
            @Bind("offset") long offset);

    @SqlUpdate("DELETE FROM blobs WHERE blob_id = :blobId")
    public abstract long deleteBlob(@Bind("blobId") long blobId);

    @SqlQuery("SELECT area, blobs.container_id, offset FROM containers, blobs WHERE blob_id = :blobId AND containers.container_id = blobs.container_id")
    @RegisterMapper(BlobLocationMapper.class)
    public abstract BlobLocation locateBlob(@Bind("blobId") long blobId);

    public static class BlobLocationMapper implements
            ResultSetMapper<BlobLocation> {
        @Override
        public BlobLocation map(int index, ResultSet r, StatementContext ctx)
                throws SQLException {
            return new BlobLocation(r.getString("area"),
                    r.getLong("container_id"),
                    r.getLong("offset"));
        }
    }

    public static class ContainerRecord {
        private final long containerId;
        private final String area;
        private final long size;
        private final boolean sealed;

        public long id() {
            return containerId;
        }

        public String area() {
            return area;
        }

        public long size() {
            return size;
        }

        public boolean sealed() {
            return sealed;
        }

        ContainerRecord(long containerId, String area, long size,
                boolean sealed) {
            this.containerId = containerId;
            this.area = area;
            this.size = size;
            this.sealed = sealed;
        }
    }

    public static class ContainerMapper implements
            ResultSetMapper<ContainerRecord> {
        @Override
        public ContainerRecord map(int index, ResultSet r, StatementContext ctx)
                throws SQLException {
            return new ContainerRecord(r.getLong("container_id"),
                    r.getString("area"), r.getLong("size"),
                    r.getBoolean("sealed"));
        }
    }

    @SqlQuery("SELECT * FROM containers WHERE container_id = :containerId")
    @RegisterMapper(ContainerMapper.class)
    public abstract ContainerRecord findContainer(
            @Bind("containerId") long containerId);

    @SqlQuery("SELECT container_id FROM containers WHERE sealed = 0 AND AREA = :area")
    public abstract Long findAnOpenContainer(@Bind("area") String area);

    @SqlUpdate("INSERT INTO containers (area) VALUES (:area)")
    @GetGeneratedKeys
    public abstract long createContainer(@Bind("area") String area);

    @SqlUpdate("UPDATE containers SET area = :area WHERE container_id = :containerId")
    public abstract long updateContainerArea(
            @Bind("containerId") long containerId, @Bind("area") String area);

    @SqlUpdate("UPDATE containers SET sealed = true WHERE container_id = :id")
    public abstract long sealContainer(@Bind("id") long containerId);

    @SqlQuery("SELECT * FROM containers")
    @RegisterMapper(ContainerMapper.class)
    public abstract Iterable<ContainerRecord> findAllContainers();

    @SqlQuery("SELECT * FROM containers WHERE sealed = true AND area = :area")
    @RegisterMapper(ContainerMapper.class)
    abstract public List<ContainerRecord> findArchivalCandidates(
            @Bind("area") String area);

    @SqlQuery("SELECT blob_id FROM legacy_paths WHERE legacy_path = :legacy_path FOR UPDATE")
    public abstract Long findBlobIdForLegacyPathAndLock(
            @Bind("legacy_path") String legacyPath);

    @SqlUpdate("INSERT INTO legacy_paths (blob_id, legacy_path) VALUES (:blob_id, :legacy_path)")
    public abstract long insertLegacy(@Bind("blob_id") long blobId,
            @Bind("legacy_path") String legacyPath);

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

    @SqlQuery("SELECT digest FROM digests WHERE blob_id = :blob_id AND algorithm = :algorithm")
    public abstract String getDigest(@Bind("blob_id") long blobId,
            @Bind("algorithm") String algorithm);

    @SqlUpdate("INSERT INTO digests (blob_id, algorithm, digest) VALUES(:blob_id, :algorithm, :digest)")
    public abstract void insertDigest(@Bind("blob_id") long blobId,
            @Bind("algorithm") String algorithm,
            @Bind("digest") String digest);

    @SqlQuery("SELECT algorithm, digest FROM digests WHERE blob_id = :blob_id")
    public abstract ResultSet getDigestsIterable(@Bind("blob_id") long blobId);

    public Map<String, String> getDigests(long blobId) {
        HashMap<String, String> out = new HashMap<String, String>();
        for (Map<String, Object> row : getHandle().select(
                "SELECT algorithm, digest FROM digests WHERE blob_id = ?",
                blobId)) {
            out.put((String) row.get("algorithm"), (String) row.get("digest"));
        }
        return out;
    }

    @SqlUpdate("INSERT INTO txs (tx_id, state, client) VALUES(:tx_id, 0, :client)")
    public abstract void insertTx(@Bind("tx_id") long txId,
            @Bind("client") String client);

    @SqlQuery("SELECT * FROM txs WHERE tx_id = :tx_id")
    @RegisterMapper(TxRecordMapper.class)
    public abstract TxRecord findTx(@Bind("tx_id") long txId);

    @SqlUpdate("UPDATE txs SET state = :state WHERE tx_id = :tx_id")
    public abstract long updateTxState(@Bind("tx_id") long txId,
            @Bind("state") long state);

    @SqlUpdate("INSERT INTO tx_blobs (tx_id, blob_id) VALUES(:tx_id, :blob_id)")
    public abstract void insertTxBlob(@Bind("tx_id") long txId,
            @Bind("blob_id") long blobId);

    @Transaction
    public void insertBlobAndTxBlob(long txId, long blobId, long containerId,
            long offset) {
        insertBlob(blobId, containerId, offset);
        insertTxBlob(txId, blobId);
    }

    @SqlQuery("SELECT blob_id FROM tx_blobs WHERE tx_id = :tx_id")
    public abstract List<Long> listBlobsByTx(@Bind("tx_id") long txId);

    @SqlQuery("SELECT 1 FROM blobs, tx_blobs, txs WHERE blobs.container_id = :container_id AND tx_blobs.blob_id = blobs.blob_id AND txs.tx_id = tx_blobs.tx_id AND (txs.state = 0) OR (txs.state = 1)")
    public abstract boolean checkContainerForOpenTxs(
            @Bind("container_id") long containerId);

    public static class TxRecord {
        public long id;
        public int state;
        public String client;
    }

    public static final int TX_OPEN = 0;
    public static final int TX_PREPARED = 1;
    public static final int TX_COMMITTED = 2;
    public static final int TX_ROLLEDBACK = 3;

    public static class TxRecordMapper implements ResultSetMapper<TxRecord> {

        @Override
        public TxRecord map(int index, ResultSet r, StatementContext ctx)
                throws SQLException {
            TxRecord tx = new TxRecord();
            tx.id = r.getLong("tx_id");
            tx.state = r.getInt("state");
            tx.client = r.getString("client");
            return tx;
        }
    }

}
