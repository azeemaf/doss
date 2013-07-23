package doss.sql;

import org.skife.jdbi.v2.DBI;


public class BlobTxIdRunningNumber extends RunningNumber {

    public BlobTxIdRunningNumber(DBI dbi) {
        super(dbi);
    }

    @Override
    IDSequenceDAO getIDSequenceDAO() {
        return dbi.onDemand(BlobTxIdDAO.class);
    }

}
