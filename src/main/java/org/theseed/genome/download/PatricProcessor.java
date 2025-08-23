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
 * --clean      remove any genomes in the output directory not in the input list
 * --sync       synchronize the target to the input list (shorthand for --missing and --clean)
 * --messages   number of seconds between status messages (default 5)
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

    /** TRUE to remove genomes not in the input list */
    @Option(name = "--clean", usage = "remove any genomes not in the input list")
    private boolean cleanOutput;

    /** TRUE to only download genomes not already in the directory */
    @Option(name = "--missing", usage = "only download new genomes")
    private boolean missingOnly;

    /** TRUE to synchronize the target to the input list */
    @Option(name = "--sync", usage = "synchronize the target to the input list (shorthand for --missing and --clean)")
    private boolean synchronize;

    /** subsystem projector file name */
    @Option(name = "--projector", metaVar = "projector.ser", usage = "optional projector file for computing subsystems")
    private File projectorFile;

    /** detail level to download */
    @Option(name = "--level", usage = "detail level needed for the genome")
    private P3Genome.Details level;

    /** number of seconds to wait between status messages on the log */
    @Option(name = "--messages", metaVar = "10", usage = "number of seconds between status messages (0 = off)")
    private int statusMessageInterval;

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
        this.synchronize = false;
        this.projectorFile = null;
        this.batchSize = 200;
        this.cleanOutput = false;
        this.statusMessageInterval = 5;
        this.level = P3Genome.Details.FULL;
    }

    @Override
    protected void validateParms() throws IOException {
        // This implements the use of --sync as a shorthand for --missing AND --clean.
        if (this.synchronize) {
            this.missingOnly = true;
            this.cleanOutput = true;
        }
        // Validate the batch size.
        if (this.batchSize < 1)
            throw new IOException("Batch size must be at least 1.");
        log.info("Output will be to {} target {}.", this.targetType, this.outDir);
        // Validate the output directory and connect to it.
        this.genomeTarget = this.targetType.create(this.outDir, this.clearOutput);
        // If we have the missing or clean option set, check for existing genomes.
        if (! this.missingOnly && ! this.cleanOutput)
            this.existingGenomes = Collections.emptySet();
        else try {
            this.existingGenomes = this.genomeTarget.getGenomeIDs();
        } catch (UnsupportedOperationException e) {
            // Here we have one of the single-file outputs that does not support figuring out what is
            // already in the target.
            log.warn("The genome target type {} does not allow the --missing or --clean option.", this.targetType);
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
        this.p3.setMessageGap(this.statusMessageInterval);
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
            // This is used to estimate load speed.
            long start = System.currentTimeMillis();
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
                if (this.missingOnly && this.existingGenomes.contains(genomeId)) {
                    skipCount++;
                    // We still have to delete it from the existing-genomes list so we
                    // don't delete it during cleanup.
                    this.existingGenomes.remove(genomeId);
                } else {
                    // Ensure there is room for this genome.
                    if (genomeIds.size() >= this.batchSize) {
                        // Process the current batch.
                        batchCount++;
                        log.info("Processing batch {} of {} genomes.", batchCount, genomeIds.size());
                        this.processBatch(genomeIds);
                        log.info("PROGRESS: {} genomes downloaded at {}ms/genome.", gCount, (System.currentTimeMillis() - start) / gCount);
                        genomeIds.clear();
                    }
                    // Add this genome ID to the current batch.
                    genomeIds.add(genomeId);
                    // Remove it from the existing-genomes list if it is there.
                    this.existingGenomes.remove(genomeId);
                }
            }
            // Process the residual batch (if any).
            if (! genomeIds.isEmpty()) {
                batchCount++;
                log.info("Processing final batch of {} genomes.", genomeIds.size());
                this.processBatch(genomeIds);
                log.info("PROGRESS: {} genomes downloaded at {}ms/genome.", gCount, (System.currentTimeMillis() - start) / gCount);
            }
            // Now we do cleanup, if necessary.
            int deleteCount = 0;
            if (this.cleanOutput && ! this.existingGenomes.isEmpty()) {
                log.info("Removing {} genomes not in input list.", this.existingGenomes.size());
                for (String genomeId : this.existingGenomes) {
                    try {
                        this.genomeTarget.remove(genomeId);
                        deleteCount++;
                    } catch (IOException e) {
                        log.warn("Could not delete genome {}: {}.", genomeId, e.getMessage());
                    }
                }
            }
            log.info("All done. {} genomes found, {} skipped, {} deleted, {} batches processed.", gCount, skipCount, deleteCount, batchCount);
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
