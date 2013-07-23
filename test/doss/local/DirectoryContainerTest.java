package doss.local;

import org.junit.Before;

import doss.core.ContainerTest;

public class DirectoryContainerTest extends ContainerTest {
    @Before
    public void setUp() throws Exception {
        container = new DirectoryContainer(0, folder.newFolder().toPath());
    }
}
