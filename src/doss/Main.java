package doss;

import static java.lang.System.err;
import static java.lang.System.out;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.thrift.transport.TTransportException;

import doss.local.LocalBlobStore;
import doss.net.BlobStoreServer;

/**
 * DOSS command-line interface
 */
@SuppressWarnings("serial")
public class Main {

    enum Command {
        help("<command>",
                "Display a list of commands or help for a given command.") {

            void listCommands() {
                out.println("usage: doss <command> [<args>]");
                out.println("");
                out.println("Available commands:");
                for (Command command : Command.values()) {
                    out.println(String.format("  %-10s%s", command.name(),
                            command.description()));
                }
            }

            @Override
            void execute(Arguments args) {
                if (args.isEmpty()) {
                    listCommands();
                } else {
                    Command.get(args.first()).usage();
                }
            }

        },
        version("", "Displays the version number.") {

            @Override
            void execute(Arguments args) throws IOException {
                if (!args.isEmpty()) {
                    usage();
                } else {
                    if (this.getClass().getPackage().getImplementationVersion() == null) {
                        out.println("DOSS 2 - unknown version\nThere is no MANIFEST.MF to look at outside of a jar.");
                    } else {
                        out.println("DOSS 2 version "
                                + this.getClass().getPackage()
                                        .getImplementationVersion());
                    }

                    try (BlobStore blobStore = openBlobStore()) {
                        if (blobStore instanceof LocalBlobStore) {
                            out.println("LocalBlobStore version: " + ((LocalBlobStore) blobStore).version());
                        }
                    }
                    out.println("Java version: "
                            + System.getProperty("java.version") + ", vendor: "
                            + System.getProperty("java.vendor"));
                    out.println("Java home: " + System.getProperty("java.home"));
                }
            }
        },
        init("", "Initialize the blob store") {
            @Override
            void execute(Arguments args) throws IOException {
                LocalBlobStore.init(getDossHome());
            }
        },
        cat("<blobId ...>", "Concatinate and print blobs (like unix cat).") {

            void outputBlob(String blobId) throws IOException {
                try (BlobStore bs = openBlobStore()) {
                    Blob blob = bs.get(Long.parseLong(blobId));
                    ReadableByteChannel channel = blob.openChannel();
                    WritableByteChannel dest = Channels.newChannel(out);

                    final ByteBuffer buffer = ByteBuffer
                            .allocateDirect(16 * 1024);

                    while (channel.read(buffer) != -1) {
                        buffer.flip();
                        dest.write(buffer);
                        buffer.compact();
                    }
                }
            }

            @Override
            void execute(Arguments args) throws IOException {
                if (args.isEmpty()) {
                    usage();
                } else {
                    for (String arg : args) {
                        outputBlob(arg);
                    }
                }
            }
        },
        digest("<algorithm> <blobId>", "Prints a digest (sha1, md5, etc) of a blob") {

            void digestBlob(String algorithm, String blobId) throws IOException {
                try (BlobStore bs = openBlobStore()) {
                    Blob blob = bs.get(Long.parseLong(blobId));
                    try {
                        out.println(blob.digest(algorithm));
                    } catch (NoSuchAlgorithmException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            @Override
            void execute(Arguments args) throws IOException {
                digestBlob(args.first(), args.rest().first());
            }
        },
        get("<blobId ...>", "Copy blobs to the current working directory.") {

            void outputBlob(String blobId) throws IOException {

                try (BlobStore bs = openBlobStore()) {
                    Blob blob = bs.get(Long.parseLong(blobId));
                    ReadableByteChannel channel = blob.openChannel();
                    FileChannel dest = FileChannel.open(Paths.get(blobId),
                            StandardOpenOption.WRITE,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING);
                    long bytesTransferred = dest.transferFrom(channel, 0,
                            Long.MAX_VALUE);
                    out.println("Got " + bytesTransferred + "B of "
                            + blob.size() + "B from blob " + blobId);
                }
            }

            @Override
            void execute(Arguments args) throws IOException {
                if (args.isEmpty()) {
                    usage();
                } else {
                    for (String arg : args) {
                        outputBlob(arg);
                    }
                }
            }
        },
        stat("[-b] <blobId ...>", "Displays metadata about blobs.") {

            String formattedTime(FileTime time) {
                Date date = new Date(time.toMillis());
                return date.toString();
            }

            @Override
            void execute(Arguments args) throws IOException {
                if (args.isEmpty()) {
                    usage();
                } else {
                    BlobStore bs = openBlobStore();

                    boolean humanSizes = true;
                    if (args.first().equals("-b")) {
                        humanSizes = false;
                        args = args.rest();
                    }

                    for (String arg : args) {
                        Blob blob = bs.get(Long.parseLong(arg));

                        out.println(blob.id() + ": ");

                        out.println("\tCreated:\t"
                                + formattedTime(blob.created()));
                        out.println("\tSize:\t\t"
                                + (humanSizes ? readableFileSize(blob.size())
                                        : blob.size() + " B"));

                        out.println("");
                    }
                }
            }
        },
        put("[-b] <file ...>", "Stores files as blobs.") {
            @Override
            void execute(Arguments args) throws IOException {
                if (args.isEmpty()) {
                    usage();
                } else {
                    BlobStore bs = openBlobStore();

                    try (BlobTx tx = bs.begin()) {

                        boolean humanSizes = true;
                        if (args.first().equals("-b")) {
                            humanSizes = false;
                            args = args.rest();
                        }

                        out.println("ID\tFilename\tSize");

                        for (String filename : args) {
                            Path p = Paths.get(filename);
                            if (!Files.exists(p)) {
                                throw new CommandLineException("no such file: "
                                        + filename);
                            }
                            if (!Files.isRegularFile(p)) {
                                throw new CommandLineException(
                                        "not a regular file: " + filename);
                            }

                            Blob blob = tx.put(p);

                            out.println(Long.toString(blob.id())
                                    + '\t'
                                    + filename
                                    + '\t'
                                    + (humanSizes ? readableFileSize(blob
                                            .size()) : blob.size() + " B"));
                        }

                        tx.commit();
                    } catch (Exception e) {
                        err.println("Aborting, rolling back all changes...");
                        throw e;
                    }

                    out.println("Created " + args.list.size() + " blobs.");
                }
            }
        },
        server("[-b bindaddr] [-p port]",
                "Run a DOSS server on the given part") {
            final OptionParser OPTION_PARSER = new OptionParser("c:");

            @Override
            void execute(Arguments args) throws IOException {
                String bindaddr = "127.0.0.1";
                int port = 7272;
                OptionSet options = args.parse("b:p:");
                if (options.has("b")) {
                    bindaddr = (String) options.valueOf("b");
                }
                if (options.has("p")) {
                    port = Integer.parseInt((String) options.valueOf("p"));
                }
                System.out.println("Opening DOSS server on " + bindaddr + ":"
                        + port);
                try (BlobStore blobStore = openBlobStore();
                        ServerSocket socket = new ServerSocket(port);
                        BlobStoreServer server = new BlobStoreServer(blobStore,
                                socket)) {
                    server.run();
                } catch (TTransportException e) {
                    throw new RuntimeException(e);
                }
            }
        },
        verify("<blobId ...>", "Checks the integrity of the given blobs") {

            void verifyBlob(String blobId) throws IOException {
                try (BlobStore bs = openBlobStore()) {
                    Blob blob = bs.get(Long.parseLong(blobId));
                    for (String error : blob.verify()) {
                        out.println("blob " + blobId + ": " + error);
                    }
                }
            }

            @Override
            void execute(Arguments args) throws IOException {
                if (args.isEmpty()) {
                    usage();
                } else {
                    for (String arg : args) {
                        verifyBlob(arg);
                    }
                }
            }
        },
        ;

        final String descrption, parameters;

        Command(String parameters, String description) {
            this.parameters = parameters;
            this.descrption = description;
        }

        abstract void execute(Arguments args) throws IOException;

        Path getDossHome() {
            String dir = System.getProperty("doss.home");
            if (dir == null) {
                throw new CommandLineException(
                        "The doss.home system property must be set, eg.: -Ddoss.home=/path/to/doss ");
            }
            ;
            return Paths.get(dir);
        }

        BlobStore openBlobStore() throws CommandLineException, IOException {
            return LocalBlobStore.open(getDossHome());
        }

        String description() {
            return this.descrption;
        }

        String parameters() {
            return this.parameters;
        }

        void usage() {
            out.println("usage: doss " + name() + " " + parameters());
            out.println(description());
        }

        static Command get(String name) {
            try {
                return valueOf(name);
            } catch (IllegalArgumentException e) {
                throw new NoSuchCommandException(name);
            }
        }
    }

    static void printError(Throwable e) {
        if (System.getProperty("doss.trace") != null) {
            e.printStackTrace();
        } else {
            err.println("doss: " + e.getLocalizedMessage());
            Throwable cause = e.getCause();
            while (cause != null) {
                err.println("caused by " + cause);
                cause = cause.getCause();
            }
        }
    }

    public static void main(String... arguments) throws IOException {
        try {
            Arguments args = new Arguments(Arrays.asList(arguments));
            if (args.isEmpty()) {
                Command.help.execute(args);
            } else {
                Command.get(args.first()).execute(args.rest());
            }
        } catch (CommandLineException e) {
            printError(e);
        } catch (CorruptBlobStoreException e) {
            printError(e);
            err.println();
            err.println("Maybe you need to run 'doss init'?");
        }
    }

    public static String readableFileSize(long size) {
        if (size <= 0)
            return "0";
        final String[] units = new String[] { "B", "KB", "MB", "GB", "TB" };
        int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
        return new DecimalFormat("#,##0.#").format(size
                / Math.pow(1024, digitGroups))
                + " " + units[digitGroups];
    }

    static class CommandLineException extends RuntimeException {
        CommandLineException(String message) {
            super(message);
        }
    }

    static class NoSuchCommandException extends CommandLineException {
        NoSuchCommandException(String command) {
            super(String.format("'%s' is not a doss command", command));
        }
    }

    static class Arguments implements Iterable<String> {
        final List<String> list;

        Arguments(List<String> list) {
            this.list = list;
        }

        @Override
        public Iterator<String> iterator() {
            return list.iterator();
        }

        boolean isEmpty() {
            return list.isEmpty();
        }

        String first() {
            return list.get(0);
        }

        Arguments rest() {
            return new Arguments(list.subList(1, list.size()));
        }

        OptionSet parse(String optionSpecification) {
            return new OptionParser(optionSpecification).parse(list
                    .toArray(new String[list.size()]));
        }
    }

}
