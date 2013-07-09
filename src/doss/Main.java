package doss;

import static java.lang.System.err;
import static java.lang.System.out;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Arrays;
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
        get("<command> <blobId>", "Prints a blob to standard output.") {
          
            void outputBlob(String blobId) throws Exception {
                out.println(blobId);
                
                if (System.getenv("DOSS_HOME") == null) {
                    throw new Exception();
                };
                
                
                Path path = new File(System.getenv("DOSS_HOME")).toPath();
                
                try (BlobStore bs = DOSS.openLocalStore(path)) {
                    Blob blob = bs.get(blobId);
                    InputStream stream = blob.openStream();
                    
                    byte[] bytes = new byte[50];
                    
                    while ( stream.read(bytes) > 0 ) {
                        out.write(bytes);
                    }
                        
                    
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                    
                
            }

            void execute(Arguments args) {
                if (args.size() != 1) {
                    usage();
                } else {
                    try {
                        outputBlob(args.first());
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }

        };
        
        final String descrption, parameters;
        
        Command(String parameters, String description) {
            this.parameters = parameters;
            this.descrption = description;
        }
        
        abstract void execute(Arguments args);
        
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
        
    public static void main(String[] arguments) {
        try {
            Arguments args = new Arguments(Arrays.asList(arguments));
            if (args.isEmpty()) {
                Command.help.execute(args);
            } else {
                Command.get(args.first()).execute(args.rest());
            }
        } catch (CommandLineException e) {
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
    
    static class Arguments {
        final List<String> list;
        
        Arguments(List<String> list) {
            this.list = list;
        }
        
        boolean isEmpty() {
            return list.isEmpty();
        }
        
        int size() {
          return list.size();
        }

        String first() {
            return list.get(0);
        }

        Arguments rest() {
            return new Arguments(list.subList(1, list.size()));
        }
    }
}
