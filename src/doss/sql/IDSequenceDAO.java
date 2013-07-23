package doss.sql;

public interface IDSequenceDAO {

    Long getNextId();

    void createSchema();

    void close();
}
