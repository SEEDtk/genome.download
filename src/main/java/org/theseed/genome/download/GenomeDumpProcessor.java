/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.Criterion;
import org.theseed.p3api.KeyBuffer;
import org.theseed.p3api.RawP3Connection;
import org.theseed.utils.BaseInputProcessor;

import com.github.cliftonlabs.json_simple.JsonObject;
import com.github.cliftonlabs.json_simple.Jsoner;

/**
 * This command will dump genomes from the BV-BRC in the form of JSON genome dump directories.
 * Each directory will contain one file for each relevant table containing a JSON array of
 * records in that table for a particular genome. Each genome will be dumped into its own
 * directory.
 *
 * The basic strategy is to dump one genome at a time. We dump the main genome file, then
 * extract the domain from the main genome record. The domain determines the other files
 * we dump.
 *
 * If a genome is not found, no output directory is created. If a table has no data for a
 * particular genome, an empty JSON list will be put in the corresponding file.
 *
 * The standard input should be tab-delimited, with headers, having genome IDs in the
 * first column. The source column can be overridden.
 *
 * The positional parameter should be the name of the output directory.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file containing genome IDs (if not STDIN)
 * -c	index (1-based) or name of input column with genome IDs (default "1")
 *
 * --batch		batch size to use for parallel processing (default 10)
 * --clear		erase output directory before processing
 * --missing	only process output directories that do not yet exist
 * --para		use parallel processing to improve performance
 *
 * @author Bruce Parrello
 *
 */
public class GenomeDumpProcessor extends BaseInputProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(GenomeDumpProcessor.class);
    /** genome list */
    private Set<String> genomes;
    /** number of genomes output */
    private int genomeCount;
    /** number of genomes skipped */
    private int skipCount;
    /** number of genomes not found or invalid */
    private int invalidCount;
    /** number of files output */
    private int fileCount;
    /** number of genomes completed */
    private int doneCount;
    /** total number of input genomes */
    private int genomeTotal;
    /** array of virus cores */
    private static final String[] VIRUS_CORES = new String[] { "genome_feature", "genome_sequence",
            "protein_feature", "protein_structure" };
    /** array of prokaryotic cores */
    private static final String[] PROK_CORES = new String[] { "bioset_result", "genome_amr",
            "genome_feature", "pathway", "ppi", "protein_structure", "sp_gene", "subsystem",
            "genome_sequence" };
    /** map of domain names to core name arrays */
    private static final Map<String, String[]> CORE_MAP = Map.of("Viruses", VIRUS_CORES,
            "Bacteria", PROK_CORES, "Archaea", PROK_CORES);

    // COMMAND-LINE OPTIONS

    /** index (1-based) or name of the input column containing genome IDs */
    @Option(name = "--col", aliases = { "-c" }, metaVar = "id_col",
            usage = "index (1-based) or name of input column containing genome IDs")
    private String idCol;

    /** if specified, the output directory will be erased before processing */
    @Option(name = "--clear", usage = "if specified, the output directory will be erased before processing")
    private boolean clearFlag;

    /** if specified, genome directories that already exist will be skipped */
    @Option(name = "--missing", usage = "if specified, genomes already in the output directory will be dumped")
    private boolean missingFlag;

    /** if specified, genomes will be dumped in parallel */
    @Option(name = "--para", usage = "if specified, genomes will be processed in parallel")
    private boolean paraFlag;

    /** batch size to use for parallel processing */
    @Option(name = "--batch", metaVar = "20", usage = "maximum number of parallel processes")
    private int batchSize;

    /** name of the output directory */
    @Argument(index = 0, metaVar = "outDir", usage = "name of the output directory", required = true)
    private File outDir;

    @Override
    protected void setReaderDefaults() {
        this.idCol = "1";
        this.clearFlag = false;
        this.missingFlag = false;
        this.batchSize = 10;
    }

    @Override
    protected void validateReaderParms() throws IOException, ParseFailureException {
        if (this.paraFlag)
            log.info("Parallel processing will be used.");
        if (this.batchSize < 1)
            throw new ParseFailureException("Batch size must be at least 1.");
        // Insure the output directory exists.
        if (! this.outDir.isDirectory()) {
            log.info("Creating output directory {}.", this.outDir);
            FileUtils.forceMkdir(outDir);
        } else if (this.clearFlag) {
            log.info("Erasing output directory {}.", this.outDir);
            FileUtils.cleanDirectory(this.outDir);
        } else
            log.info("Genomes will be dumped to {}.", this.outDir);
    }

    @Override
    protected void validateReaderInput(TabbedLineReader reader) throws IOException {
        // Here we get the set of genome IDs to read.
        this.genomes = new HashSet<String>(1000);
        // Find the input column index.
        int idColIdx = reader.findField(this.idCol);
        // Loop through the input file.
        log.info("Reading genome IDs from input.");
        for (var line : reader) {
            String genomeId = line.get(idColIdx);
            this.genomes.add(genomeId);
        }
        this.genomeTotal = this.genomes.size();
        log.info("{} genomes found in input.", this.genomeTotal);
    }

    @Override
    protected void runReader(TabbedLineReader reader) throws Exception {
        // Clear the counters.
        this.genomeCount = 0;
        this.invalidCount = 0;
        this.skipCount = 0;
        this.fileCount = 0;
        this.doneCount = 0;
        // Create the batch holder.
        List<String> batch = new ArrayList<String>(this.batchSize);
        // Loop through the genome IDs, processing batches.
        for (String genomeId : this.genomes) {
            // Insure there is room for this genome.
            if (batch.size() >= this.batchSize) {
                this.processBatch(batch);
                batch.clear();
            }
            batch.add(genomeId);
        }
        // Process the residual batch (if any).
        if (! batch.isEmpty())
            this.processBatch(batch);
        // Output the counters.
        log.info("{} genomes processed: {} invalid, {} skipped. {} files output.",
                this.genomeCount, this.invalidCount, this.skipCount, this.fileCount);
    }

    /**
     * Process the specified batch of genomes.
     *
     * @param batch		list of genomes to process
     */
    private void processBatch(Collection<String> batch) {
        this.makePara(batch.stream(), this.paraFlag).forEach(x -> this.processGenome(x));
    }

    /**
     * This method dumps a single genome. Since it might be running in parallel mode,
     * it has its own private P3 connection.
     *
     * @param genomeId	ID of the genome to dump
     */
    private void processGenome(String genomeId) {
        try {
            RawP3Connection p3 = new RawP3Connection();
            // Get the index number of this genome.
            int gNum;
            synchronized (this) {
                this.genomeCount++;
                gNum = this.genomeCount;
            }
            // These flags are used to update the counts at the end.
            boolean skipped = false;
            boolean invalid = false;
            int files = 0;
            // Create the output directory name.
            File genomeDir = new File(this.outDir, genomeId);
            // Check to see if we need to skip this genome.
            if (this.missingFlag && genomeDir.isDirectory()) {
                log.info("Genome {} already dumped-- skipping.", genomeId);
                skipped = true;
            } else {
                // We need to dump the genome. Get its genome record.
                String selectGenome = Criterion.EQ("genome_id", genomeId);
                List<JsonObject> jsonList = p3.getRecords("genome", selectGenome);
                if (jsonList.isEmpty()) {
                    log.warn("Genome {} does not exist.", genomeId);
                    skipped = true;
                } else {
                    // Get the genome and its name.
                    JsonObject genomeJson = jsonList.get(0);
                    String genomeName = KeyBuffer.getString(genomeJson, "genome_name");
                    // Determine the domain.
                    String domain = KeyBuffer.getString(genomeJson, "superkingdom");
                    String[] coreNames = CORE_MAP.get(domain);
                    if (coreNames == null) {
                        log.warn("Genome {} has an unsupported domain \"{}\".", genomeId, domain);
                        invalid = true;
                    } else {
                        // Here we are at the point we can actually dump the files. First, create
                        // the output directory and write the genome file.
                        log.info("Dumping genome {} of {}: {} {}", gNum, this.genomeTotal, genomeId,
                                genomeName);
                        FileUtils.forceMkdir(genomeDir);
                        this.writeFile(genomeDir, "genome", jsonList);
                        files++;
                        // For log purposes, get the total number of files we're dumping.
                        final int nFiles = coreNames.length + 1;
                        // Now loop through the other cores.
                        for (String coreName : coreNames) {
                            // Check for an odd criterion.
                            String criterion1;
                            switch (coreName) {
                            case "ppi" :
                                criterion1 = Criterion.EQ("genome_id_a", genomeId);
                                break;
                            case "epitope" :
                                criterion1 = Criterion.EQ("organism", genomeName);
                                break;
                            default:
                                criterion1 = selectGenome;
                            }
                            // Get the genome's records.
                            jsonList = p3.getRecords(coreName, criterion1);
                            this.writeFile(genomeDir, coreName, jsonList);
                            files++;
                            log.info("File {} of {} for {} completed.", files, nFiles, genomeId);
                        }
                    }
                }
            }
            // Update the counters.
            synchronized(this) {
                this.fileCount += files;
                if (skipped) this.skipCount++;
                if (invalid) this.invalidCount++;
                this.doneCount++;
            }
            log.info("Dump completed for {} of {} genomes.", this.doneCount, this.genomeTotal);
        } catch (IOException e) {
            // All exceptions from this method must be unchecked, since it is used in a stream.
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Write the specified json list to the specified file.
     *
     * @param genomeDir	output genome directory
     * @param coreName	name of core from which the JSON objects were dumped
     * @param jsonList	list of JSON objects to write
     *
     * @throws IOException
     */
    private void writeFile(File genomeDir, String coreName, List<JsonObject> jsonList) throws IOException {
        File outFile = new File(genomeDir, coreName + ".json");
        try (PrintWriter writer = new PrintWriter(outFile)) {
            // Write the leading bracket.
            writer.println("[");
            // Loop through the list, writing JSON lines.
            Iterator<JsonObject> iter = jsonList.iterator();
            while (iter.hasNext()) {
                JsonObject json = iter.next();
                String jsonString = Jsoner.serialize(json);
                writer.print(jsonString);
                // We append a comma to all but the last entry.
                if (iter.hasNext())
                    writer.print(",");
                writer.println();
            }
            // Write the trailing bracket.
            writer.println("]");
        }
    }

}
