package doss;

import static java.lang.System.err;
import static java.lang.System.out;

import java.io.IOException;
import java.nio.channels.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.channels.FileChannel;
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
                if (System.getProperty("doss.home") == null) {
                    throw new CommandLineException("the DOSS_HOME environment variable must be set");
                };
                                
                Path path = Paths.get( System.getProperty("doss.home") );
                
                BlobStore bs = DOSS.openLocalStore(path);
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
        get("<blobId ...>", "Copy blobs to the current working directory.") {
          
            void outputBlob(String blobId) throws IOException {
                if (System.getProperty("doss.home") == null) {
                    throw new CommandLineException("the DOSS_HOME environment variable must be set");
                };

                BlobStore bs = DOSS.openLocalStore(Paths.get( System.getProperty("doss.home") ));
                Blob blob = bs.get(blobId);
                ReadableByteChannel channel = blob.openChannel();
                
                Path outputFile = Files.createFile(Paths.get(System.getProperty("user.dir") + "/" + blob.id()));
                WritableByteChannel dest = FileChannel.open(outputFile);
                        
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

        };
        
        final String descrption, parameters;
        
        Command(String parameters, String description) {
            this.parameters = parameters;
            this.descrption = description;
        }
        
        abstract void execute(Arguments args) throws IOException;
        
        String description() {
            return this.descrption;
        }
        
        void usage() {
            out.println("usage: doss " + name());
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
