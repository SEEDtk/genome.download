/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.Connection;
import org.theseed.p3api.P3Genome;
import org.theseed.utils.BaseProcessor;
import org.theseed.utils.ICommand;

/**
/**
 * This downloads genomes from PATRIC and converts them into a GenomeDirectory; that is, a directory of GTOs.
 *
 * The positional parameter is the name of the output directory.
 *
 * The standard input should be a tab-delimited file containing a list of PATRIC genome IDs.  The default is
 * to look for them in a column named "genome_id".
 *
 * The command-line options are as follows.
 *
 * -h		display usage information
 * -v		display more progress information on the log
 * -c		index (1-based) or name of the input column containing the genome IDs
 * -i		if specifed, the name of a file to be used as the input (instead of STDIN)
 *
 * --clear		erase the output directory before proceeding
 *
 * @author Bruce Parrello
 *
 */
public class PatricProcessor extends BaseProcessor implements ICommand {

    // FIELDS
    /** genome ID input stream */
    private TabbedLineReader inStream;
    /** genome ID input column */
    private int colIdx;

    // COMMAND-LINE OPTIONS

    @Option(name = "-c", aliases = { "--col", "--column" }, metaVar = "1", usage = "index (1-based) or name of genome ID column")
    private String column;

    @Option(name = "-i", aliases = { "--input" }, metaVar = "genomeList.tbl", usage = "name of input file (if not STDIN)")
    private File inFile;

    /** TRUE to clear the output directory during initialization */
    @Option(name = "--clear", usage = "erase output directory before starting")
    private boolean clearOutput;

    /** name of the output directory */
    @Argument(index = 0, metaVar = "outDir", usage = "name of the output directory", required = true)
    private File outDir;

    @Override
    protected void setDefaults() {
        this.column = "genome_id";
        this.inFile = null;
        this.clearOutput = false;
    }

    @Override
    protected boolean validateParms() throws IOException {
        // Validate the output directory.
        if (! this.outDir.exists()) {
            // Here we must create it.
            log.info("Creating output directory {}.", this.outDir);
            if (! this.outDir.mkdirs())
                throw new IOException("Could not create output directory " + this.outDir + ".");
        } else if (! this.outDir.isDirectory()) {
            throw new FileNotFoundException("Output directory " + this.outDir + " is invalid.");
        } else if (this.clearOutput) {
            // Here we have to erase the directory before we put new stuff in.
            log.info("Clearing output directory {}.", this.outDir);
            FileUtils.cleanDirectory(this.outDir);
        }
        // Set up the input.
        if (this.inFile == null) {
            log.info("Reading genome IDs from standard input.");
            this.inStream = new TabbedLineReader(System.in);
        } else if (! this.inFile.canRead()) {
            throw new FileNotFoundException("Input file " + this.inFile + " not found or unreadable.");
        } else {
            log.info("Reading genome IDs from {}.", this.inFile);
            this.inStream = new TabbedLineReader(this.inFile);
        }
        // Find the input column.
        this.colIdx = this.inStream.findField(this.column);
        return true;
    }

    @Override
    public void runCommand() throws Exception {
        // Connect to PATRIC.
        Connection p3 = new Connection();
        try {
            // Count the genomes written.
            int gCount = 0;
            // Loop through the input file, processing genome IDs.
            for (TabbedLineReader.Line line : this.inStream) {
                String genomeId = line.get(this.colIdx);
                log.info("Processing {}.", genomeId);
                P3Genome genome = P3Genome.Load(p3, genomeId, P3Genome.Details.FULL);
                if (genome == null)
                    log.error("Genome {} not found.", genomeId);
                else {
                    File outFile = new File(this.outDir, genomeId + ".gto");
                    log.info("Writing {} to {}.", genome, outFile);
                    genome.update(outFile);
                    gCount++;
                }
            }
            log.info("All done. {} genomes output.", gCount);
        } finally {
            // Close the input file.
            this.inStream.close();
        }
    }

}
