package doss.sql;

import org.skife.jdbi.v2.DBI;


public class BlobIdRunningNumber extends RunningNumber {

    public BlobIdRunningNumber(DBI dbi) {
        super(dbi);
    }

    @Override
    IDSequenceDAO getIDSequenceDAO() {
        return dbi.onDemand(BlobIdDAO.class);
    }

}
