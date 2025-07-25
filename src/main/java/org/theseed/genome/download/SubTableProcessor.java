/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.genome.Genome;
import org.theseed.subsystems.SubsystemRowDescriptor;

/**
 * This command reads a genome file and produces a table of subsystem information as output.  The table can be read
 * by other commands for rapid conversion of feature IDs to subsystem lists.
 *
 * The positional parameters are the name of the genome file and the name for the output file.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	produce more detailed log messages
 *
 * @author Bruce Parrello
 */
public class SubTableProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SubTableProcessor.class);

    // COMMAND-LINE OPTIONS

    /** input file */
    @Argument(index = 0, metaVar = "inFile", usage = "name of the input genome file")
    private File genomeFile;

    /** output file */
    @Argument(index = 1, metaVar = "outFile", usage = "name of the output directory file")
    private File outFile;

    @Override
    protected void setDefaults() {
    }

    @Override
    protected void validateParms() throws IOException, ParseFailureException {
        if (! genomeFile.canRead())
            throw new FileNotFoundException("Genome file " + this.genomeFile + " not found or unreadable.");
    }

    @Override
    protected void runCommand() throws Exception {
        log.info("Reading genome from {}.", this.genomeFile);
        Genome genome = new Genome(this.genomeFile);
        log.info("Parsing subsystems in {}.", genome);
        SubsystemRowDescriptor.createFile(genome, this.outFile);
    }

}
