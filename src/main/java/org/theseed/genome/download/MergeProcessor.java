package org.theseed.genome.download;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.genome.Genome;
import org.theseed.genome.GenomeMultiDirectory;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.P3CursorConnection;
import org.theseed.p3api.P3Genome;
import org.theseed.p3api.P3GenomeBatch;

/**
 * This command takes a directory of genome ID files and downloads them from the BV-BRC into a genome master directory. 
 * If a genome is listed multiple times, it will only be downloaded once.
 * 
 * The first positional parameter should be the input directory name.  The second positional parameter should
 * be the output master directory name. By default, the genome lists will be taken from "*.tbl" files in the
 * input directory, with the genome IDs presumed to be in the first column of each file.
 * 
 * Note that if "--clear" is not used, then existing files in the output directory will be preserved and not 
 * overwritten. In addition, extra genomes may be present in the output directory that are not listed in the 
 * input files.
 * 
 * Regardless of which files are used, they must all be tab-delimited with headers.
 * 
 * The command-line options are as follows:
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
 * -c   index (1-based) or name of the column containing genome IDs (default "1")
 * -b   batch size for downloads (default 100)
 * 
 * --level      detail level to download (default is FULL)
 * --filter     filename pattern for selecting input files (default is "*.tbl")
 * --clear		erase the output directory before proceeding
 * 
 */
public class MergeProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(MergeProcessor.class);
    /** genome master directory controller */
    private GenomeMultiDirectory masterDir;
    /** array of input files */
    private File[] inputFiles;
    /** number of genomes output */
    private int genomeCount;
    /** connection to the BV-BRC database */
    private P3CursorConnection p3;

    // COMMAND-LINE OPTIONS

    /** index (1-based) or name of the column containing genome IDs (default "1") */
    @Option(name = "--col", aliases = { "-c" }, metaVar = "genome_id", usage = "index (1-based) or name of the column containing genome IDs")
    private String genomeIdCol;

    /** batch size for downloads (default 100) */
    @Option(name = "--batch", aliases = { "-b" }, metaVar = "200", usage = "batch size for downloads")
    private int batchSize;

    /** TRUE to clear the output directory during initialization */
    @Option(name = "--clear", usage = "erase output directory before starting")
    private boolean clearOutput;

    /** detail level to download */
    @Option(name = "--level", usage = "detail level needed for the genome")
    private P3Genome.Details level;

    /** filename pattern for selecting input files */
    @Option(name = "--filter", metaVar = "genomes*.tsv", usage = "filename pattern for selecting input files")
    private String filter;

    /** input directory containing genome ID files */
    @Argument(index = 0, metaVar = "inDir", usage = "input directory containing genome ID files", required = true)
    private File inDir;

    /** output master directory for genomes */
    @Argument(index = 1, metaVar = "outDir", usage = "output master directory for genomes", required = true)
    private File outDir;


    @Override
    protected void setDefaults() {
        this.genomeIdCol = "1";
        this.batchSize = 100;
        this.clearOutput = false;
        this.level = P3Genome.Details.FULL;
        this.filter = "*.tbl";
    }

    @Override
    protected void validateParms() throws IOException, ParseFailureException {
        // Verify that the input directory is valid.
        if (! this.inDir.isDirectory())
            throw new IOException("Input directory " + this.inDir + " is not found or invalid.");
        // Insure we have input files.
        WildcardFileFilter fileFilter = WildcardFileFilter.builder().setWildcards(this.filter).get();
        this.inputFiles = this.inDir.listFiles((FileFilter) fileFilter);
        if (this.inputFiles == null || this.inputFiles.length == 0)
            throw new IOException("No input files found in " + this.inDir + " matching " + this.filter);
        log.info("{} input files found in {}", this.inputFiles.length, this.inDir);
        // Validate the tuning parameters.
        if (this.batchSize <= 0)
            throw new ParseFailureException("Batch size must be positive: " + this.batchSize);
        // Create the output directory manager.
        if (this.outDir.isDirectory() && ! this.clearOutput) {
            log.info("Connecting to existing output directory " + this.outDir);
            this.masterDir = new GenomeMultiDirectory(outDir);
        } else
            this.masterDir = GenomeMultiDirectory.create(outDir, this.clearOutput);
    }

    @Override
    protected void runCommand() throws Exception {
        // Connect to the BV-BRC database.
        this.p3 = new P3CursorConnection();
        // We will accumulate the next batch of genome IDs to download in here.
        Set<String> batch = new HashSet<>(this.batchSize * 3 / 2 + 1);
        // Count the number of input files processed.
        int fileCount = 0;
        // Count the number of genomes output.
        this.genomeCount = 0;
        // Count the number of genome IDs read.
        int lineCount = 0;
        // Count the number of batches processed.
        int batchCount = 0;
        // Loop through the input files.
        for (File inputFile : this.inputFiles) {
            fileCount++;
            log.info("Processing input file {} of {}: {}", fileCount, this.inputFiles.length, inputFile);
            // Open the input file for reading and get the input column index.
            try (TabbedLineReader inStream = new TabbedLineReader(inputFile)) {
                int genomeIdIdx = inStream.findField(this.genomeIdCol);
                for (var line : inStream) {
                    lineCount++;
                    String genomeId = line.get(genomeIdIdx);
                    if (! this.masterDir.contains(genomeId)) {
                        batch.add(genomeId);
                        if (batch.size() >= this.batchSize) {
                            this.downloadBatch(batch);
                            batch.clear();
                            batchCount++;
                            log.info("Processed batch {} of size {}", batchCount, this.batchSize);
                        }
                    }
                }
            }
        }
        // Process any remaining genomes in the last batch.
        if (! batch.isEmpty()) {
            this.downloadBatch(batch);
            batchCount++;
            log.info("Processed final batch {} of size {}", batchCount, batch.size());
        }
        log.info("Processed {} input files with {} genome IDs. Output {} genomes in {} batches.", 
                fileCount, lineCount, this.genomeCount, batchCount);
    }

    /**
     * Download and output a batch of genomes.
     * 
     * @param batch     the set of genome IDs for the genomes to download
     * 
     * @throws IOException if an error occurs in the database or the output directory
     */
    private void downloadBatch(Set<String> batch) throws IOException {
        // Download the batch of genomes.
        P3GenomeBatch genomes = new P3GenomeBatch(this.p3, batch, this.level);
        log.info("{} genomes downloaded.", genomes.size());
        // Output the downloaded genomes to the master directory.
        for (Genome genome : genomes) {
            this.masterDir.add(genome);
            genomeCount++;
        }
        log.info("{} genomes now in master directory.", this.masterDir.size());
    }

}
