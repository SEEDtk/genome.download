package org.theseed.genome.download;

import java.util.Arrays;

import org.theseed.utils.BaseProcessor;

/**
 * Download genomes into genome directories.
 *
 * subTable		create a subsystem directory file for a genome
 * fasta		download a FASTA file from the PATRIC website
 * fastaDir		download a directory of FASTA files from the PATRIC website
 * vipr			create the genomes from ViPR download files
 * patric		download the genomes from the PATRIC website
 * core			extract genomes from a SEED organism directory
 * ncbi			create a genome from NCBI download files
 * md5			compute MD5s from FASTA files
 * subsystems	compute a subsystem projector for the CoreSEED
 * seed			compress the SEED into an output file
 * project		project subsystems onto GTOs
 * copy			copy genomes from a source to a local directory
 * list			list the IDs of all the genomes in a genome source
 * sync			synchronize a genome target with a list of PATRIC genome IDs
 * subMap		create a map from subsystem IDs to subsystem names
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
        case "fastaDir" :
            processor = new FastaDirProcessor();
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
        case "project" :
            processor = new ProjectionProcessor();
            break;
        case "copy" :
            processor = new GenomeCopyProcessor();
            break;
        case "list" :
            processor = new MasterListProcessor();
            break;
        case "sync" :
            processor = new SynchronizeProcessor();
            break;
        case "subMap" :
            processor = new SubMapProcesor();
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
