package doss;

import static java.lang.System.*;

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

            void execute(List<String> args) {
                if (args.size() > 0) {
                    Command.get(args.get(0)).usage();
                } else {
                    listCommands();
                }
            }

        };
        
        final String descrption, parameters;
        
        Command(String parameters, String description) {
            this.parameters = parameters;
            this.descrption = description;
        }
        
        abstract void execute(List<String> args);
        
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
            List<String> args = Arrays.asList(arguments);
            if (args.size() > 0) {
                String cmd = args.get(0);
                List<String> options = args.subList(1, args.size());
                Command.get(cmd).execute(options);
            } else {
                Command.help.execute(args.subList(0, 0));
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
}
