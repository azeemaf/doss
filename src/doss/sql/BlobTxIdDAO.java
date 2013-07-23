package doss.sql;

import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;

public interface BlobTxIdDAO extends IDSequenceDAO {

    @Override
    @SqlQuery("SELECT NEXTVAL('BLOB_TX_ID_SEQ')")
    Long getNextId();

    @Override
    @SqlUpdate("CREATE SEQUENCE IF NOT EXISTS BLOB_TX_ID_SEQ")
    void createSchema();

    @Override
    void close();
}
