package doss.local;

import org.junit.After;
import org.junit.Before;

public class DirectoryContainerTest extends ContainerTest {
    private Database db;

    @Before
    public void setUp() throws Exception {
        db = Database.open();
        db.migrate();
        container = new DirectoryContainer(db, 0, folder.newFolder().toPath());
    }

    @After
    public void closeDb() {
        db.close();
    }
}
