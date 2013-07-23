package doss.sql;

public class BlobTxIdDAOTest extends IDSequenceDAOTest {

    @Override
    IDSequenceDAO getDAO() {
        return dbi.onDemand(BlobTxIdDAO.class);
    }

}
