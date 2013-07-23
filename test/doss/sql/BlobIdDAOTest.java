package doss.sql;

public class BlobIdDAOTest extends IDSequenceDAOTest {

    @Override
    IDSequenceDAO getDAO() {
        return dbi.onDemand(BlobIdDAO.class);
    }

}
