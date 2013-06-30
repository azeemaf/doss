package doss;

import static java.lang.System.*;

import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;

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

            void execute(Deque<String> args) {
                if (args.isEmpty()) {
                    listCommands();
                } else {
                    Command.get(args.pop()).usage();
                } 
            }

        };
        
        final String descrption, parameters;
        
        Command(String parameters, String description) {
            this.parameters = parameters;
            this.descrption = description;
        }
        
        abstract void execute(Deque<String> args);
        
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
            Deque<String> args = new LinkedList<>(Arrays.asList(arguments));
            if (args.isEmpty()) {
                Command.help.execute(args);
            } else {
                Command.get(args.pop()).execute(args);
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
