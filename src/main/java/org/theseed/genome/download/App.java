package org.theseed.genome.download;

import java.util.Arrays;

import org.theseed.utils.ICommand;

/**
 * Download genomes into genome directories.
 *
 * vipr		create the genomes from ViPR download files
 *
 */
public class App
{
    public static void main( String[] args ) {
        // Get the control parameter.
        String command = args[0];
        String[] newArgs = Arrays.copyOfRange(args, 1, args.length);
        ICommand processor;
        // Determine the command to process.
        switch (command) {
        case "vipr" :
            processor = new ViprProcessor();
            break;
        default:
            throw new RuntimeException("Invalid command " + command);
        }
        // Process it.
        boolean ok = processor.parseCommand(newArgs);
        if (ok) {
            processor.run();
        }
    }
}
