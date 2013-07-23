package doss.sql;

import org.skife.jdbi.v2.DBI;

abstract public class RunningNumber implements doss.local.RunningNumber {

    final protected DBI dbi;

    RunningNumber(final DBI dbi) {
        this.dbi = dbi;
    }

    public long next() {
        return getIDSequenceDAO().getNextId();
    }

    abstract IDSequenceDAO getIDSequenceDAO();
}
