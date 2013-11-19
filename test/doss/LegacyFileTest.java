package doss;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;

import org.bouncycastle.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import doss.local.LocalBlobStore;

public class LegacyFileTest {

    public static final String TEST_STRING = "test string";
    public static final byte[] TEST_BYTES =
            TEST_STRING.getBytes(Charset.forName("UTF-8"));
    public static String legacyPath;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public BlobStore blobStore;
    public Path blobStoreRoot;

    public BlobStore remoteBlobStore;

    @Before
    public void openBlobStore() throws IOException {

        // set up blob store
        blobStoreRoot = folder.newFolder().toPath();
        LocalBlobStore.init(blobStoreRoot);
        blobStore = LocalBlobStore.open(blobStoreRoot);
        // remoteBlobStore = RemoteBlobStore.open(socket)

        // set up test legacy file in a sub folder of temp folder
        File legacyDossFolder = folder.newFolder("legacyDoss");
        File legacyFile = new File(legacyDossFolder, "LEGACY-FILE");
        FileWriter fw = new FileWriter(legacyFile);
        fw.append("abc");
        fw.close();

        legacyPath = legacyFile.toPath().toString();
    }

    @After
    public void closeBlobStore() throws Exception {
        blobStore.close();
        blobStore = null;
    }

    @Test
    public void testGetLegacy() throws Exception {

        // get a legacy file using its path
        Blob b = blobStore.getLegacy(new File(legacyPath).toPath());
        assertEquals(3l, b.size());

        // put the same legacy file in the blob store so it can be referenced
        // via a blob id
        LocalBlobStore.Tx tx = (LocalBlobStore.Tx) blobStore.begin();
        Long id = tx.putLegacy(new File(legacyPath).toPath());
        s("Stored legacy blob and got back id " + id);

        // retrieve blob via id
        Blob b2 = blobStore.get(id);

        // compare contents of both blobs
        InputStream is = b.openStream();
        byte[] ba = new byte[(int) b.size()];
        is.read(ba);

        InputStream is2 = b2.openStream();
        byte[] ba2 = new byte[(int) b2.size()];
        is2.read(ba2);

        assertTrue(Arrays.areEqual(ba, ba2));

    }

    private static void s(String s) {
        System.out.println(s);
    }
}
