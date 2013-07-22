package doss.sql;

import org.skife.jdbi.v2.sqlobject.SqlUpdate;

public interface ContainerIndexSchemaDAO {

    @SqlUpdate("CREATE TABLE IF NOT EXISTS container_index (blob_id BIGINT PRIMARY KEY, offset BIGINT)")
    void createSchema();

    void close();
}
