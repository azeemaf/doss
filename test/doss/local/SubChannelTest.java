package doss.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@SuppressWarnings("unused")
public class SubChannelTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    SeekableByteChannel channel1;
    Path testPath;

    @After
    public void tearDown() throws Exception {

    }

    @Before
    public void setUp() throws Exception {
        testPath = FileSystems.getDefault().getPath(
                folder.newFolder().toPath().toString(), "test");
        writeFileBytes(testPath.toString(), getSomeContent());
    }

    @Test
    public void checkReadFromBegining() throws Exception {

        try (SeekableByteChannel sbt = Files.newByteChannel(testPath,
                EnumSet.of(StandardOpenOption.READ))) {

            channel1 = new SubChannel(sbt, 0, 25);
            ByteBuffer buffer = ByteBuffer.allocate(15);
            String encoding = "UTF-8";
            // reads first 15 bytes
            int r1 = channel1.read(buffer);
            buffer.flip();
            String s1 = Charset.forName(encoding).decode(buffer).toString();
            assertEquals(
                    "Checking that it reads frm the right part of the file ",
                    "All the world's", s1);
            buffer.clear();
            int r2 = channel1.read(buffer);
            assertEquals("Checking reading remaining bytes in the subchannel",
                    10, r2);

            buffer.flip();
            String s2 = Charset.forName(encoding).decode(buffer).toString();
            assertEquals("Checking that remaining bytes read are correct ",
                    " a stage,A", s2);

            buffer.clear();
            // can not read any more, all bytes are read
            int r3 = channel1.read(buffer);
            assertEquals("No more bytes to read, bytest read should be 0", 0,
                    r3);

        }

    }

    @Test
    public void checkReadMiddle() throws Exception {

        try (SeekableByteChannel sbt = Files.newByteChannel(testPath,
                EnumSet.of(StandardOpenOption.READ))) {

            channel1 = new SubChannel(sbt, 25, 25);
            ByteBuffer buffer = ByteBuffer.allocate(15);
            String encoding = "UTF-8";
            // reads first 15 bytes
            int r1 = channel1.read(buffer);
            buffer.flip();
            String s1 = Charset.forName(encoding).decode(buffer).toString();
            assertEquals(
                    "Checking that it reads frm the right part of the file ",
                    "nd all the men ", s1);
            buffer.clear();
            int r2 = channel1.read(buffer);
            assertEquals("Checking reading remaining bytes in the subchannel",
                    10, r2);
            buffer.flip();
            String s2 = Charset.forName(encoding).decode(buffer).toString();
            assertEquals("Checking that remaining bytes read are correct ",
                    "and women ", s2);

            buffer.clear();
            // can not read any more, all bytes are read
            int r3 = channel1.read(buffer);
            assertEquals("No more bytes to read, bytest read should be 0", 0,
                    r3);

        }

    }

    @Test(expected = IllegalArgumentException.class)
    public void checkConstructorForIllegalLength() throws Exception {

        try (SeekableByteChannel sbt = Files.newByteChannel(testPath,
                EnumSet.of(StandardOpenOption.READ))) {

            channel1 = new SubChannel(sbt, 100, 150);

        }

    }

    @Test(expected = IllegalArgumentException.class)
    public void checkConstructorForIllegalOffset() throws Exception {

        try (SeekableByteChannel sbt = Files.newByteChannel(testPath,
                EnumSet.of(StandardOpenOption.READ))) {

            channel1 = new SubChannel(sbt, 150, 60);

        }

    }

    @Test(expected = NullPointerException.class)
    public void checkConstructorForNullContainerChannel() throws Exception {

        try (SeekableByteChannel sbt = Files.newByteChannel(testPath,
                EnumSet.of(StandardOpenOption.READ))) {

            channel1 = new SubChannel(null, 150, 60);

        }

    }

    @Test
    public void testPosition() {

        try (SeekableByteChannel sbt = Files.newByteChannel(testPath,
                EnumSet.of(StandardOpenOption.READ))) {

            channel1 = new SubChannel(sbt, 25, 25);
            SeekableByteChannel sbt1 = channel1.position(15);
            ByteBuffer buffer = ByteBuffer.allocate(7);
            String encoding = "UTF-8";
            int r1 = channel1.read(buffer);
            buffer.flip();
            String s1 = Charset.forName(encoding).decode(buffer).toString();
            System.out.println(s1);
            assertEquals(
                    "Checking if we are reading from the correct position ",
                    "and wom", s1);

        } catch (IOException ioe) {
            fail(ioe.getClass().getName() + " not expected");
        }

    }

    @Test
    public void testReadLimit() {

        try (SeekableByteChannel sbt = Files.newByteChannel(testPath,
                EnumSet.of(StandardOpenOption.READ))) {

            channel1 = new SubChannel(sbt, 25, 25);
            SeekableByteChannel sbt1 = channel1.position(15);
            ByteBuffer buffer = ByteBuffer.allocate(100);
            String encoding = "UTF-8";
            int r1 = channel1.read(buffer);
            buffer.flip();
            String s1 = Charset.forName(encoding).decode(buffer).toString();
            System.out.println(s1);
            assertEquals(
                    "Checking if we are reading from the correct position ",
                    "and women ", s1);

        } catch (IOException ioe) {
            fail(ioe.getClass().getName() + " not expected");
        }

    }

    @Test(expected = IllegalArgumentException.class)
    public void testPositionBeforeSubChannelStart() {

        try (SeekableByteChannel sbt = Files.newByteChannel(testPath,
                EnumSet.of(StandardOpenOption.READ))) {

            channel1 = new SubChannel(sbt, 25, 25);
            SeekableByteChannel sbt1 = channel1.position(-20);

        } catch (IOException ioe) {
            fail("IOException not expected");
        }

    }

    @Test(expected = IllegalArgumentException.class)
    public void testPositionAfterChannelEnd() {

        try (SeekableByteChannel sbt = Files.newByteChannel(testPath,
                EnumSet.of(StandardOpenOption.READ))) {

            channel1 = new SubChannel(sbt, 25, 25);
            SeekableByteChannel sbt1 = channel1.position(52);

        } catch (IOException ioe) {
            fail("IOException not expected");
        }

    }

    @Test(expected = NonWritableChannelException.class)
    public void testWrite() throws IOException {

        try (SeekableByteChannel sbt = Files.newByteChannel(testPath,
                EnumSet.of(StandardOpenOption.READ))) {
            channel1 = new SubChannel(sbt, 1, 25);
            String s = "traralaah";
            final byte[] bytes = s.getBytes();
            channel1.write(ByteBuffer.wrap(bytes));
        }
    }

    @Test(expected = NonWritableChannelException.class)
    public void tesTruncatee() throws IOException {

        try (SeekableByteChannel sbt = Files.newByteChannel(testPath,
                EnumSet.of(StandardOpenOption.READ))) {
            channel1 = new SubChannel(sbt, 1, 25);
            channel1.truncate(5);
        }
    }

    private static void writeFileBytes(String filename, String content) {
        try {
            Files.write(FileSystems.getDefault().getPath(filename),
                    content.getBytes("UTF-8"), StandardOpenOption.CREATE);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private static String getSomeContent() {
        String content = "All the world's a stage,"
                + "And all the men and women merely players;"
                + "They have their exits and their entrances,"
                + "And one man in his time plays many parts,";
        return content;
    }

}
