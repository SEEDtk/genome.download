package org.theseed.genome.download;

import java.util.Arrays;

import org.theseed.utils.BaseProcessor;

/**
 * Download genomes into genome directories.
 *
 * vipr			create the genomes from ViPR download files
 * patric		download the genomes from the PATRIC website
 * core			extract genomes from a SEED organism directory
 * ncbi			create a genome from NCBI download files
 * md5			compute MD5s from FASTA files
 * subsystems	apply CoreSEED subsystems to GTOs
 * seed			create a testing subset for CoreSEED
 * fasta		convert FASTA files into skeleton GTOs
 * subTable		create a subsystem directory file for a genome
 *
 */
public class App
{
    public static void main( String[] args ) {
        // Get the control parameter.
        String command = args[0];
        String[] newArgs = Arrays.copyOfRange(args, 1, args.length);
        BaseProcessor processor;
        // Determine the command to process.
        switch (command) {
        case "fasta" :
            processor = new FastaProcessor();
            break;
        case "ncbi" :
            processor = new GffProcessor();
            break;
        case "vipr" :
            processor = new ViprProcessor();
            break;
        case "patric" :
            processor = new PatricProcessor();
            break;
        case "md5" :
            processor = new ChecksumProcessor();
            break;
        case "core" :
            processor = new CoreProcessor();
            break;
        case "subsystems" :
            processor = new SubsystemProcessor();
            break;
        case "seed" :
            processor = new SeedProcessor();
            break;
        case "subTable" :
            processor = new SubTableProcessor();
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
