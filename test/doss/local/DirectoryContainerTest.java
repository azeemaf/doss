package doss.local;

import org.junit.Before;

public class DirectoryContainerTest extends ContainerTest {
    @Before
    public void setUp() throws Exception {
        container = new DirectoryContainer(folder.newFolder().toPath());
    }
}
