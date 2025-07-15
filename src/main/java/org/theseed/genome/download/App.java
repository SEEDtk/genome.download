package org.theseed.genome.download;

import java.util.Arrays;

import org.theseed.basic.BaseProcessor;

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
 * sraMap		list SRA samples corresponding to PATRIC genomes
 * sraFasta		create a FASTA pseudo-sample directory from sraMap output
 * project1		project subsystems into a single GTO
 * dump			dump genomes from BV-BRC
 *
 */
public class App
{
    /** static array containing command names and comments */
    protected static final String[] COMMANDS = new String[] {
             "subTable", "create a subsystem directory file for a genome",
             "fasta", "download a FASTA file from the PATRIC website",
             "fastaDir", "download a directory of FASTA files from the PATRIC website",
             "vipr", "create the genomes from ViPR download files",
             "patric", "download the genomes from the PATRIC website",
             "core", "extract genomes from a SEED organism directory",
             "ncbi", "create a genome from NCBI download files",
             "md5", "compute MD5s from FASTA files",
             "subsystems", "compute a subsystem projector for the CoreSEED",
             "seed", "compress the SEED into an output file",
             "project", "project subsystems onto GTOs",
             "copy", "copy genomes from a source to a local directory",
             "list", "list the IDs of all the genomes in a genome source",
             "sync", "synchronize a genome target with a list of PATRIC genome IDs",
             "subMap", "create a map from subsystem IDs to subsystem names",
             "sraMap", "list SRA samples corresponding to PATRIC genomes",
             "sraFasta", "create a FASTA pseudo-sample directory from sraMap output",
             "dump", "dump genomes from BV-BRC"
    };

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
        case "project1" :
            processor = new Project1Processor();
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
        case "sraMap" :
            processor = new SraMapProcessor();
            break;
        case "sraFasta" :
            processor = new SraFastaProcessor();
            break;
        case "dump" :
        	processor = new GenomeDumpProcessor();
        	break;
        case "-h" :
        case "--help" :
            processor = null;
            break;
        default :
            throw new RuntimeException("Invalid command " + command + ".");
        }
        if (processor == null)
            BaseProcessor.showCommands(COMMANDS);
        else {
            processor.parseCommand(newArgs);
            processor.run();
        }
    }
}
