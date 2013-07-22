package doss.sql;

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;

public interface ContainerIndexDAO {

    @SqlQuery("SELECT offset FROM container_index WHERE blob_id = :blobId")
    Long locate(@Bind("blobId") long blobId);

    @SqlUpdate("INSERT INTO container_index (blob_id, offset) VALUES (:blobId, :offset)")
    void insert(@Bind("blobId") long blobId, @Bind("offset") long offset);

    @SqlUpdate("UPDATE container_index SET offset = :offset WHERE blob_id = :blobId")
    void update(@Bind("blobId") long blobId, @Bind("offset") long offset);

    @SqlUpdate("DELETE FROM container_index WHERE blob_id = :blobId")
    void delete(@Bind("blobId") long blobId);
}
