package doss.local;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.compress.utils.Charsets;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.logging.PrintStreamLog;
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

import doss.Client;
import doss.NoSuchBlobException;

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
        if (System.getenv("DOSS_LOG_SQL") != null) {
            dbi.setSQLLog(new PrintStreamLog());
        }
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
        if (jdbcUrl != null) {
            flyway.setDataSource(jdbcUrl, null, null);
        } else {
            flyway.setDataSource(md.getURL() + H2_SWITCHES, md.getUserName(), "");
        }
        flyway.setLocations("doss/migrations");
        return flyway;
    }

    public String version() {
        try {
            Flyway flyway = openFlyway();
            String current = flyway.info().current().getVersion().toString();
            int npending = flyway.info().pending().length;
            if (npending > 0) {
                return current + " (" + npending + " migrations pending)";
            }
            return current;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @SqlQuery("SELECT NEXTVAL('ID_SEQ')")
    public abstract long nextId();

    @SqlUpdate("ALTER SEQUENCE ID_SEQ INCREMENT BY :delta")
    public abstract void increaseBlobIdSequence(@Bind("delta") long delta);

    @SqlUpdate("INSERT INTO blobs (blob_id, container_id, offset) VALUES (:blobId, :containerId, :offset)")
    public abstract void insertBlob(@Bind("blobId") long blobId,
            @Bind("containerId") long containerId,
            @Bind("offset") long offset);

    @SqlUpdate("INSERT INTO blobs (blob_id, tx_id) VALUES (:blobId, :txId)")
    public abstract void insertBlob(@Bind("blobId") long blobId, @Bind("txId") long txId);

    @SqlUpdate("DELETE FROM blobs WHERE blob_id = :blobId")
    public abstract long deleteBlob(@Bind("blobId") long blobId);

    @SqlQuery("SELECT blobs.blob_id,  blobs.container_id, offset, state, blobs.tx_id FROM blobs LEFT JOIN containers ON containers.container_id = blobs.container_id WHERE blob_id = :blobId ")
    @RegisterMapper(BlobLocationMapper.class)
    public abstract BlobLocation locateBlob(@Bind("blobId") long blobId);

    @SqlQuery("SELECT blobs.blob_id, blobs.container_id, offset, state, blobs.tx_id FROM blobs LEFT JOIN containers ON containers.container_id = blobs.container_id")
    @RegisterMapper(BlobLocationMapper.class)
    public abstract Iterable<BlobLocation> locateAllBlobs();

    public static class BlobLocationMapper implements
            ResultSetMapper<BlobLocation> {
        @Override
        public BlobLocation map(int index, ResultSet r, StatementContext ctx)
                throws SQLException {
            return new BlobLocation(r.getLong("blob_id"),
                    (Long) r.getObject("container_id"),
                    (Long) r.getObject("offset"),
                    (Integer) r.getObject("state"),
                    (Long) r.getObject("tx_id"));
        }
    }

    public static class ContainerRecord {
        private final long containerId;
        private final long size;
        private final int state;

        public long id() {
            return containerId;
        }

        public long size() {
            return size;
        }

        ContainerRecord(long containerId, long size, int state) {
            this.containerId = containerId;
            this.size = size;
            this.state = state;
        }

        public String stateName() {
            switch (state) {
            case CNT_OPEN:
                return "OPEN";
            case CNT_SEALED:
                return "SEALED";
            case CNT_ARCHIVED:
                return "ARCHIVED";
            case CNT_WRITTEN:
                return "WRITTEN";
            default:
                return "UNKNOWN_" + state;
            }
        }

        public int state() {
            return state;
        }
    }

    public static class ContainerMapper implements
            ResultSetMapper<ContainerRecord> {
        @Override
        public ContainerRecord map(int index, ResultSet r, StatementContext ctx)
                throws SQLException {
            return new ContainerRecord(r.getLong("container_id"),
                    r.getLong("size"), r.getInt("state"));
        }
    }

    @SqlQuery("SELECT * FROM containers WHERE container_id = :containerId")
    @RegisterMapper(ContainerMapper.class)
    public abstract ContainerRecord findContainer(
            @Bind("containerId") long containerId);

    @SqlQuery("SELECT container_id FROM containers WHERE state = 0")
    public abstract Long findAnOpenContainer();

    @SqlUpdate("INSERT INTO containers (state) VALUES (" + CNT_OPEN + ")")
    @GetGeneratedKeys
    public abstract long createContainer();

    @SqlQuery("SELECT * FROM containers")
    @RegisterMapper(ContainerMapper.class)
    public abstract Iterable<ContainerRecord> findAllContainers();

    @SqlQuery("SELECT blobs.blob_id FROM blobs, txs WHERE blobs.tx_id = txs.tx_id AND blobs.container_id IS NULL AND txs.state = "
            + TX_COMMITTED)
    abstract public List<Long> findCommittedButUnassignedBlobs();

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

    @SqlQuery("SELECT * FROM clients")
    public abstract List<Client> listClients();

    public static class ClientMapper implements ResultSetMapper<Client> {

        @Override
        public Client map(int index, ResultSet r, StatementContext ctx)
                throws SQLException {
            return new Client(r.getLong("client_id"), r.getString("name"),
                    r.getString("host"), r.getString("access"),
                    r.getString("public_key"));
        }

    }

    @SqlQuery("SELECT digest FROM digests WHERE blob_id = :blobId AND algorithm = :algorithm")
    public abstract String getDigest(@Bind("blobId") long blobId,
            @Bind("algorithm") String algorithm);

    @SqlUpdate("INSERT INTO digests (blob_id, algorithm, digest) VALUES(:blobId, :algorithm, :digest)")
    public abstract void insertDigest(@Bind("blobId") long blobId,
            @Bind("algorithm") String algorithm, @Bind("digest") String digest);

    public Map<String, String> getDigests(long blobId) {
        HashMap<String, String> out = new HashMap<String, String>();
        for (Map<String, Object> row : getHandle().select(
                "SELECT algorithm, digest FROM digests WHERE blob_id = ?",
                blobId)) {
            out.put((String) row.get("algorithm"), (String) row.get("digest"));
        }
        return out;
    }

    @SqlUpdate("INSERT INTO txs (tx_id, state, client, opened) VALUES(:tx_id, 0, :client, CURRENT_TIMESTAMP())")
    public abstract void insertTx(@Bind("tx_id") long txId,
            @Bind("client") String client);

    @SqlQuery("SELECT * FROM txs WHERE tx_id = :tx_id")
    @RegisterMapper(TxRecordMapper.class)
    public abstract TxRecord findTx(@Bind("tx_id") long txId);

    @SqlUpdate("UPDATE txs SET state = :state, closed = CURRENT_TIMESTAMP() WHERE tx_id = :tx_id")
    public abstract long updateTxState(@Bind("tx_id") long txId,
            @Bind("state") long state);

    @Transaction
    public void insertBlobAndTxBlob(long txId, long blobId) {
        insertBlob(blobId, txId);
    }

    @SqlUpdate("UPDATE containers SET size = size + :delta WHERE container_id = :container_id")
    public abstract int increaseContainerSize(@Bind("container_id") long containerId,
            @Bind("delta") long delta);

    @SqlUpdate("UPDATE containers SET size = :size WHERE container_id = :container_id")
    public abstract int setContainerSize(@Bind("container_id") long containerId,
            @Bind("size") long size);

    @SqlQuery("SELECT size FROM containers WHERE container_id = :container_id")
    public abstract long getContainerSize(@Bind("container_id") long containerId);

    @SqlQuery("SELECT last_offset FROM containers WHERE container_id = :container_id")
    public abstract Long getContainerLastOffset(@Bind("container_id") long containerId);

    @SqlQuery("SELECT blob_id FROM blobs WHERE tx_id = :tx_id")
    public abstract List<Long> listBlobsByTx(@Bind("tx_id") long txId);

    public static class TxRecord {
        public long id;
        public int state;
        public String client;
        public Date opened;
        public Date closed;
    }

    public static final int TX_OPEN = 0;
    public static final int TX_PREPARED = 1;
    public static final int TX_COMMITTED = 2;
    public static final int TX_ROLLEDBACK = 3;
    public static final int TX_ROLLINGBACK = 4;

    public static final int CNT_OPEN = 0;
    public static final int CNT_SEALED = 1;
    public static final int CNT_WRITTEN = 2;
    public static final int CNT_ARCHIVED = 3;

    public static class TxRecordMapper implements ResultSetMapper<TxRecord> {

        @Override
        public TxRecord map(int index, ResultSet r, StatementContext ctx)
                throws SQLException {
            TxRecord tx = new TxRecord();
            tx.id = r.getLong("tx_id");
            tx.state = r.getInt("state");
            tx.client = r.getString("client");
            tx.opened = r.getDate("opened");
            tx.closed = r.getDate("closed");
            return tx;
        }
    }

    @SqlUpdate("UPDATE blobs SET container_id = :container_id WHERE blob_id = :blob_id")
    public abstract int setBlobContainerId(@Bind("blob_id") long blobId,
            @Bind("container_id") long containerId);

    @Transaction
    public void addBlobToContainer(long blobId, Long containerId, long size) {
        if (setBlobContainerId(blobId, containerId) != 1) {
            throw new NoSuchBlobException(blobId);
        }
        if (increaseContainerSize(containerId, size) != 1) {
            throw new NoSuchContainerException(containerId);
        }
    }

    @SqlUpdate("UPDATE containers SET state = :state WHERE container_id = :container_id")
    public abstract int updateContainerState(@Bind("container_id") long containerId,
            @Bind("state") int cntSelected);

    @SqlQuery("SELECT * FROM containers WHERE state = :state")
    public abstract List<Long> findContainersByState(@Bind("state") long state);

    @SqlQuery("SELECT blob_id FROM blobs WHERE container_id = :container_id")
    public abstract List<Long> findBlobsByContainer(@Bind("container_id") long containerId);

    @SqlUpdate("UPDATE containers SET state = " + CNT_SEALED + " WHERE state = " + CNT_OPEN)
    public abstract int sealAllContainers();

    @SqlUpdate("UPDATE blobs SET offset = :offset WHERE blob_id = :blob_id")
    public abstract int setBlobOffset(@Bind("blob_id") long blobId, @Bind("offset") long offset);

    @SqlUpdate("INSERT INTO container_digests (container_id, algorithm, digest) VALUES(:containerId, :algorithm, :digest)")
    public abstract void insertContainerDigest(@Bind("containerId") long containerId,
            @Bind("algorithm") String algorithm, @Bind("digest") String digest);

    @SqlQuery("SELECT digest FROM container_digests WHERE container_id = :containerId AND algorithm = :algorithm")
    public abstract String getContainerDigest(@Bind("containerId") long containerId,
            @Bind("algorithm") String algorithm);

    public Map<String, String> getContainerDigests(long containerId) {
        HashMap<String, String> out = new HashMap<String, String>();
        for (Map<String, Object> row : getHandle().select(
            "SELECT algorithm, digest FROM container_digests WHERE container_id = ?", containerId)) {
            out.put((String) row.get("algorithm"), (String) row.get("digest"));
        }
        return out;
    }

    @SqlQuery("SELECT result FROM digest_audits WHERE container_id = :containerId ORDER by time DESC LIMIT 1")
    public abstract boolean getLastAuditResult(@Bind("containerId") long containerId);

    @SqlQuery("SELECT time FROM digest_audits WHERE container_id = :containerId ORDER by time DESC LIMIT 1")
    public abstract Timestamp getLastAuditTime(@Bind("containerId") long containerId);

    @SqlUpdate("INSERT INTO digest_audits (container_id, algorithm, time, result) VALUES(:containerId, :algorithm, :time, :result)")
    public abstract void insertAuditResult(@Bind("containerId") long containerId,
            @Bind("algorithm") String algorithm, @Bind("time") Date time, @Bind("result") boolean result);

    @SqlQuery("SELECT x.container_id FROM digest_audits x INNER JOIN ( SELECT container_id,MAX(time) mtime FROM digest_audits GROUP BY container_id) y ON x.container_id = y.container_id AND x.time = y.mtime AND result = false")
    public abstract List<Long> getFailedAudits();
}
