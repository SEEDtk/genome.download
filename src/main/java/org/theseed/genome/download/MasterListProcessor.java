/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseReportProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.genome.iterator.GenomeSource;

/**
 * This command does a fast list of all the genome IDs for the genomes in a single source.  This
 * is considerably faster than reading each genome and doing the more comprehensive list using
 * a copy to a target of type LIST.
 *
 * The positional parameter is the name of the genome source directory or file.  The command-line
 * options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o	output file for the list (if not STDOUT)
 * -t	type of genome source (PATRIC, MASTER, DIR); the default is DIR
 *
 * @author Bruce Parrello
 *
 */
public class MasterListProcessor extends BaseReportProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(MasterListProcessor.class);
    /** genome source */
    private GenomeSource genomes;

    // COMMAND-LINE OPTIONS

    /** type of genome source */
    @Option(name = "--source", aliases = { "-t", "--type" }, usage = "type of genome input (master genome directory, GTO directory, patric ID file)")
    private GenomeSource.Type inType;

    /** input genome source */
    @Argument(index = 0, metaVar = "inDir", usage = "input genome file or directory", required = true)
    private File inDir;

    @Override
    protected void setReporterDefaults() {
        this.inType = GenomeSource.Type.DIR;
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
        if (! this.inDir.exists())
            throw new FileNotFoundException("Could not find genome source " + this.inDir + ".");
        // Create the genome source.
        this.genomes = this.inType.create(this.inDir);
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        // Loop through the genome IDs, writing them out.
        int count = 0;
        writer.println("genome_id");
        for (String genomeId : this.genomes.getIDs()) {
            writer.println(genomeId);
            count++;
        }
        log.info("{} genome IDs written.", count);
    }

}
