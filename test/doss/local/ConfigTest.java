package doss.local;

import static org.junit.Assert.assertEquals;
import java.util.*;

import org.junit.Test;

import doss.DOSSTest;
import doss.local.Database.ContainerRecord;

public class ConfigTest extends DOSSTest {

    @Test
    public void ensureContainersCanBeSealed() throws Exception {
        LocalBlobStore blobStore = (LocalBlobStore) this.blobStore;
		assertEquals(2,blobStore.masterRoots.size());

		{
			List<String> algorithms = blobStore.algorithms;
			assertEquals(2, algorithms.size());
		}
    }
}
