package doss.sql;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

public class Tools {

    public static void dropAllObjects(DBI dbi) {
        Handle h = null;
        try {
            h = dbi.open();
            h.execute("DROP ALL OBJECTS");
        } finally {
            if (h != null) {
                h.close();
            }
        }
    }
}
