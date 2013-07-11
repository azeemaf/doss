package doss.local;

import org.skife.jdbi.v2.sqlobject.SqlUpdate;

public interface BlobIndexSchemaDAO {

    @SqlUpdate("CREATE TABLE IF NOT EXISTS blob_index (blob_id BIGINT PRIMARY KEY, offset BIGINT)")
    void createSchema();

    void close();
}
