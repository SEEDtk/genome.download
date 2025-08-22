/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseProcessor;
import org.theseed.genome.Genome;
import org.theseed.genome.iterator.GenomeTargetType;
import org.theseed.genome.iterator.IGenomeTarget;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.P3CursorConnection;
import org.theseed.p3api.P3Genome;
import org.theseed.p3api.P3GenomeBatch;
import org.theseed.subsystems.core.SubsystemRuleProjector;


/**
 * This downloads genomes from PATRIC and stores them in a genome target, which is a directory or file of
 * genome data.
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
 * -b       batch size for downloads (default 200)
 *
 * --target     type of genome target; the default is DIR (GTO directory)
 * --clear		erase the output directory before proceeding
 * --missing	only download genomes not already present
 * --projector	if specified, the name of a subsystem projector file for computing the subsystems
 * --level		detail level to download; the default is FULL
 *
 * @author Bruce Parrello
 *
 */
public class PatricProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(PatricProcessor.class);
    /** genome ID input stream */
    private TabbedLineReader inStream;
    /** genome ID input column */
    private int colIdx;
    /** subsystem projector */
    private SubsystemRuleProjector projector;
    /** connection to PATRIC */
    private P3CursorConnection p3;
    /** output genome target */
    private IGenomeTarget genomeTarget;
    /** set of genomes already in the target */
    private Set<String> existingGenomes;

    // COMMAND-LINE OPTIONS

    /** index (1-based) or name of genome ID column */
    @Option(name = "-c", aliases = { "--col", "--column" }, metaVar = "1", usage = "index (1-based) or name of genome ID column")
    private String column;

    /** name of input file (if not STDIN) */
    @Option(name = "-i", aliases = { "--input" }, metaVar = "genomeList.tbl", usage = "name of input file (if not STDIN)")
    private File inFile;

    /** batch size for downloads */
    @Option(name = "-b", aliases = { "--batch" }, metaVar = "100", usage = "batch size for downloads")
    private int batchSize;

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

    /** type of genome target */
    @Option(name = "--target", usage = "type of genome target; the default is DIR (GTO directory)")
    private GenomeTargetType targetType;

    /** name of the output directory */
    @Argument(index = 0, metaVar = "outDir", usage = "name of the output directory", required = true)
    private File outDir;

    @Override
    protected void setDefaults() {
        this.column = "genome_id";
        this.inFile = null;
        this.targetType = GenomeTargetType.DIR;
        this.clearOutput = false;
        this.missingOnly = false;
        this.projectorFile = null;
        this.batchSize = 200;
        this.level = P3Genome.Details.FULL;
    }

    @Override
    protected void validateParms() throws IOException {
        // Validate the batch size.
        if (this.batchSize < 1)
            throw new IOException("Batch size must be at least 1.");
        log.info("Output will be to {} target {}.", this.targetType, this.outDir);
        // Validate the output directory and connect to it.
        this.genomeTarget = this.targetType.create(this.outDir, this.clearOutput);
        // If we have the missing option set, check for existing genomes.
        if (! this.missingOnly)
            this.existingGenomes = Collections.emptySet();
        else try {
            this.existingGenomes = this.genomeTarget.getGenomeIDs();
        } catch (UnsupportedOperationException e) {
            // Here we have one of the single-file outputs that does not support figuring out what is
            // already in the target.
            log.warn("The genome target type {} does not allow the --missing option.", this.targetType);
            this.existingGenomes = Collections.emptySet();
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
            // Count the genome IDs read.
            int gCount = 0;
            // Count the genomes skipped.
            int skipCount = 0;
            // Count the number of batches.
            int batchCount = 0;
            // This will hold the current batch of genome IDs.
            Set<String> genomeIds = new HashSet<>(this.batchSize * 4 / 3 + 1);
            // Loop through the input file, processing genome IDs.
            for (TabbedLineReader.Line line : this.inStream) {
                String genomeId = line.get(this.colIdx);
                gCount++;
                // Only proceed if we are NOT skipping this genome.
                if (this.existingGenomes.contains(genomeId))
                    skipCount++;
                else {
                    // Ensure there is room for this genome.
                    if (genomeIds.size() >= this.batchSize) {
                        // Process the current batch.
                        batchCount++;
                        log.info("Processing batch {} of {} genomes.", batchCount, genomeIds.size());
                        this.processBatch(genomeIds);
                        genomeIds.clear();
                    }
                    // Add this genome ID to the current batch.
                    genomeIds.add(genomeId);
                }
            }
            // Process the residual batch (if any).
            if (! genomeIds.isEmpty()) {
                batchCount++;
                log.info("Processing final batch of {} genomes.", genomeIds.size());
                this.processBatch(genomeIds);
            }
            log.info("All done. {} genomes found, {} skipped, {} batches processed.", gCount, skipCount, batchCount);
        } finally {
            // Close the input file.
            this.inStream.close();
        }
    }

    /**
     * Load a batch of genomes and store them in the target. We also do subsystem projection here.
     * 
     * @param genomeIds     IDs of the genomes to load.
     * 
     * @throws IOException 
     */
    private void processBatch(Set<String> genomeIds) throws IOException {
        P3GenomeBatch batch = new P3GenomeBatch(this.p3, genomeIds, this.level);
        // Loop through the genomes.
        for (Genome genome : batch) {
            if (this.projector != null)
                this.projector.project(genome, true);
            this.genomeTarget.add(genome);
        }
    }

}
