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
import org.theseed.basic.BaseProcessor;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.P3CursorConnection;
import org.theseed.p3api.P3Genome;
import org.theseed.subsystems.core.SubsystemRuleProjector;


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
 * --missing	only download genomes not already present
 * --projector	if specified, the name of a subsystem projector file for computing the subsystems
 * --subsystems	if specified, subsystems will be downloaded from PATRIC
 * --level		detail level to download; the default is FULL
 *
 * @author Bruce Parrello
 *
 */
public class PatricProcessor extends BaseProcessor {

    // FIELDS
    /** genome ID input stream */
    private TabbedLineReader inStream;
    /** genome ID input column */
    private int colIdx;
    /** subsystem projector */
    private SubsystemRuleProjector projector;
    /** connection to PATRIC */
    private P3CursorConnection p3;

    // COMMAND-LINE OPTIONS

    @Option(name = "-c", aliases = { "--col", "--column" }, metaVar = "1", usage = "index (1-based) or name of genome ID column")
    private String column;

    @Option(name = "-i", aliases = { "--input" }, metaVar = "genomeList.tbl", usage = "name of input file (if not STDIN)")
    private File inFile;

    /** TRUE to clear the output directory during initialization */
    @Option(name = "--clear", usage = "erase output directory before starting")
    private boolean clearOutput;

    /** TRUE to only download genomes not already in the directory */
    @Option(name = "--missing", usage = "only download new genomes")
    private boolean missingOnly;

    /** subsystem projector file name */
    @Option(name = "--projector", metaVar = "projector.ser", usage = "optional projector file for computing subsystems")
    private File projectorFile;

    /** detail level to download */
    @Option(name = "--level", usage = "detail level needed for the genome")
    private P3Genome.Details level;

    /** name of the output directory */
    @Argument(index = 0, metaVar = "outDir", usage = "name of the output directory", required = true)
    private File outDir;

    @Override
    protected void setDefaults() {
        this.column = "genome_id";
        this.inFile = null;
        this.clearOutput = false;
        this.missingOnly = false;
        this.projectorFile = null;
        this.level = P3Genome.Details.FULL;
    }

    @Override
    protected void validateParms() throws IOException {
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
        // Connect to PATRIC.
        this.p3 = new P3CursorConnection();
        // Set up the subsystem projector.
        this.projector = null;
        if (this.projectorFile != null) {
            log.info("Loading subsystem projector from {}.", this.projectorFile);
            this.projector = SubsystemRuleProjector.load(this.projectorFile);
        }
        // Find the input column.
        this.colIdx = this.inStream.findField(this.column);
    }

    @Override
    public void runCommand() throws Exception {
        try {
            // Count the genomes written.
            int gCount = 0;
            // Loop through the input file, processing genome IDs.
            for (TabbedLineReader.Line line : this.inStream) {
                String genomeId = line.get(this.colIdx);
                log.info("Processing {}.", genomeId);
                File outFile = new File(this.outDir, genomeId + ".gto");
                if (this.missingOnly && outFile.exists()) {
                    log.info("{} already present-- skipped.", outFile);
                } else {
                    P3Genome genome = P3Genome.load(this.p3, genomeId, this.level);
                    if (genome == null)
                        log.error("Genome {} not found.", genomeId);
                    else {
                        // Project subsystems if we have a projector. Note we specify TRUE
                        // to restrict to active subsystems.
                        if (this.projector != null)
                            this.projector.project(genome, true);
                        // Write the genome.
                        log.info("Writing {} to {}.", genome, outFile);
                        genome.save(outFile);
                        gCount++;
                    }
                }
            }
            log.info("All done. {} genomes output.", gCount);
        } finally {
            // Close the input file.
            this.inStream.close();
        }
    }

}
