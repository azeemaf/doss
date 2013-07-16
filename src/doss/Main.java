package doss;

import static java.lang.System.err;
import static java.lang.System.out;

import java.io.IOException;
import java.nio.channels.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/**
 * DOSS command-line interface
 */
@SuppressWarnings("serial")
public class Main {
    
    enum Command {
        help("<command>", "Display a list of commands or help for a given command.") {
            
            void listCommands() {
                out.println("usage: doss <command> [<args>]");
                out.println("");
                out.println("Available commands:");
                for (Command command: Command.values()) {
                    out.println(String.format("  %-10s%s", command.name(), command.description()));
                }                
            }

            void execute(Arguments args) {
                if (args.isEmpty()) {
                    listCommands();
                } else {
                    Command.get(args.first()).usage();
                } 
            }

        },        
        cat("<blobId ...>", "Concatinate and print blobs (like unix cat).") {
          
            void outputBlob(String blobId) throws IOException {
                BlobStore bs = DOSS.openLocalStore(basedir());
                Blob blob = bs.get(blobId);
                ReadableByteChannel channel = blob.openChannel();
                WritableByteChannel dest = Channels.newChannel(out);

                final ByteBuffer buffer = ByteBuffer.allocateDirect(16 * 1024);

                while (channel.read(buffer) != -1) {
                    buffer.flip();
                    dest.write(buffer);
                    buffer.compact();
                }
                buffer.flip();
                while (buffer.hasRemaining()) {
                    dest.write(buffer);
                }
            }

            void execute(Arguments args) throws IOException {
                if (args.isEmpty()) {
                    usage();
                } else {
                    for (String arg: args) {
                        outputBlob(arg);
                    }
                }
            }
        },
        put("<file ...>", "Stores files as blobs.") {

            void execute(Arguments args) throws IOException {
                if (args.isEmpty()) {
                    usage();
                } else {
                    BlobStore bs = DOSS.openLocalStore(basedir());

                    try (BlobTx tx = bs.begin()) {

                        out.println("ID\tfilename\tsize");

                        for (String filename: args) {
                            Blob blob = tx.put(Paths.get(filename));
                            out.println(blob.id() + '\t' + filename + '\t' + blob.size() + "B");
                        }

                        tx.commit();
                    }
                }
            }
        };
        
        final String descrption, parameters;
        
        Command(String parameters, String description) {
            this.parameters = parameters;
            this.descrption = description;
        }
        
        abstract void execute(Arguments args) throws IOException;
        
        Path basedir() throws CommandLineException {
            if (System.getProperty("doss.home") == null) {
                throw new CommandLineException("The doss.home system property must be set, eg.: -Ddoss.home=/path/to/doss ");
            };
                            
            return Paths.get( System.getProperty("doss.home") );
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
    
    public static void main(String... arguments) {
        try {
            Arguments args = new Arguments(Arrays.asList(arguments));
            if (args.isEmpty()) {
                Command.help.execute(args);
            } else {
                Command.get(args.first()).execute(args.rest());
            }
        } catch (CommandLineException | IOException e) {
            err.println("doss: " + e.getLocalizedMessage());
            e.printStackTrace();
        }
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
    }


}
