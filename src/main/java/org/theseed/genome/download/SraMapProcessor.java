/**
 *
 */
package org.theseed.genome.download;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.ncbi.NcbiConnection;
import org.theseed.ncbi.NcbiListQuery;
import org.theseed.ncbi.NcbiTable;
import org.theseed.ncbi.XmlException;
import org.theseed.ncbi.XmlUtils;
import org.theseed.p3api.P3Connection;
import org.theseed.p3api.P3Connection.Table;
import org.theseed.utils.BasePipeProcessor;
import org.w3c.dom.Element;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This command will accept as input a list of PATRIC genome IDs and return the corresponding SRA
 * samples from the NCBI.  These samples can then be downloades using the NCBI sample downloader.
 * This is a tricky business.  The PATRIC database will give us a comma-delimited list of run accession
 * IDs.  The NCBI service will return an XML document containing experiments.  We need to find the
 * run IDs in the experiment descriptions and connect them back to the PATRIC IDs.  We can then
 * produce a report showing the genome ID, name, and experiment ID.
 *
 * Many of the run accession numbers listed in PATRIC will not exist in the NCBI any more.  In that
 * case, the genome will not be output.  The result is that the output file can be fed into a batch
 * process for downloading samples with little extra work.
 *
 * The genome IDs should come from the standard input and the report will be produced on the standard
 * output.  By default, the genome IDs will be presumed to be in the first column of the input, which
 * should be tab-delimited with headers.
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file containing genome IDs (if not STDIN)
 * -o	output file for report (if not STDOUT)
 * -b	batch size for PATRIC queries (default 50)
 * -c	index (1-based) or name of input column with genome IDs (default "1")
 *
 * @author Bruce Parrello
 *
 */
public class SraMapProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SraMapProcessor.class);
    /** genome names for current batch */
    private Map<String, String> nameMap;
    /** genome IDs for current batch */
    private Set<String> batch;
    /** map of run accession IDs to genome IDs for the current batch */
    private Map<String, String> runMap;
    /** input ID column index */
    private int idColIdx;
    /** connection to PATRIC */
    private P3Connection p3;
    /** connection to NCBI */
    private NcbiConnection ncbi;
    /** list query for runs */
    private NcbiListQuery runQuery;
    /** number of genomes without SRA data */
    private int runlessCount;
    /** number of experiments found */
    private int expCount;

    // COMMAND-LINE OPTIONS

    /** PATRIC query batch size */
    @Option(name = "--batchSize", aliases = { "-b" }, metaVar = "100", usage = "batch size for PATRIC queries")
    private int batchSize;

    /** input column identifier */
    @Option(name = "--col", aliases = { "-c" }, metaVar = "genome_id",
            usage = "index (1-based) or name of input column containing genome IDs")
    private String inCol;

    @Override
    protected void setPipeDefaults() {
        this.batchSize = 50;
        this.inCol = "1";
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
        if (this.batchSize < 1)
            throw new ParseFailureException("Batch size must be at least 1.");
        // Initialize the batch hashes.
        this.nameMap = new HashMap<String, String>(this.batchSize * 4 / 3 + 1);
        this.runMap = new HashMap<String, String>(this.batchSize * 2);
        this.batch = new HashSet<String>(this.batchSize * 3 / 2 + 1);
        // Connect to the online databases.
        log.info("Connecting to PATRIC.");
        this.p3 = new P3Connection();
        log.info("Connecting to NCBI.");
        this.ncbi = new NcbiConnection();
        this.runQuery = new NcbiListQuery(NcbiTable.SRA, "ACCN");
    }

    @Override
    protected void validatePipeInput(TabbedLineReader inputStream) throws IOException {
        this.idColIdx = inputStream.findField(this.inCol);
    }

    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        // Write the output header.
        writer.println("genome_id\tgenome_name\tsrx_id");
        // Initialize the counters.
        int lineIn = 0;
        int batchCount = 0;
        this.runlessCount = 0;
        this.expCount = 0;
        // Loop through the input stream, forming batches.
        for (var line : inputStream) {
            String genome_id = line.get(this.idColIdx);
            lineIn++;
            if (this.batch.size() >= this.batchSize) {
                batchCount++;
                // Here the current batch is full and we need to process it.
                this.processBatch(writer);
                // Set up for the next batch.
                this.batch.clear();
                this.runMap.clear();
                this.nameMap.clear();
                log.info("{} total genomes processed.  {} experiments found.", lineIn, this.expCount);
            }
            // Add the new genome ID to the batch.
            this.batch.add(genome_id);
        }
        if (this.batch.size() > 0) {
            batchCount++;
            this.processBatch(writer);
        }
        log.info("{} lines read, {} batches processed.", lineIn, batchCount);
        log.info("{} genomes had no SRA data.  {} total experiments found.", this.runlessCount,
                this.expCount);
    }

    /**
     * This method processes a single batch of genomes.  First, it queries PATRIC for the
     * genome name and SRA accession attributes.  This fills the name map and the run map.
     * Then we query NCBI for the experiment data related to the runs.  For every experiment
     * we find, we write an output line.
     *
     * @param writer	output print writer
     *
     * @throws IOException
     * @throws XmlException
     */
    private void processBatch(PrintWriter writer) throws XmlException, IOException {
        log.info("Requesting PATRIC data on {} genomes.", this.batch.size());
        var records = this.p3.getRecords(Table.GENOME, batch, "genome_name,sra_accession");
        log.info("{} records returned from PATRIC.", records.size());
        // Process each record, filling in the name and run maps.
        for (var recordEntry : records.entrySet()) {
            String genomeId = recordEntry.getKey();
            JsonObject record = recordEntry.getValue();
            String sraString = P3Connection.getString(record, "sra_accession");
            if (StringUtils.isBlank(sraString))
                this.runlessCount++;
            else {
                // Here we have runs to process.  Save the genome name.
                String genomeName = P3Connection.getString(record, "genome_name");
                this.nameMap.put(genomeId, genomeName);
                // Extract the runs and map them to the genome.
                String[] runs = StringUtils.split(sraString, ',');
                for (String run : runs)
                    this.runMap.put(run, genomeId);
            }
        }
        log.info("{} runs found for {} genomes.", this.runMap.size(), this.nameMap.size());
        // Now we must ask NCBI for information about the runs.  Many of the runs will not
        // be found, but the rest will be output.
        this.runQuery.addIds(this.runMap.keySet());
        var elements = this.ncbi.query(runQuery);
        log.info("{} experiments returned from NCBI query.", elements.size());
        // For each element returned, we need to find the runs.  This determines the relevant genome.
        // The elements returned are EXPERIMENT_PACKAGE tags.
        for (Element element : elements) {
            Element experimentData = XmlUtils.getFirstByTagName(element, "EXPERMENT");
            String experimentId = experimentData.getAttribute("accession");
            Element runSet = XmlUtils.getFirstByTagName(element, "RUN_SET");
            // The runs are children of the run set.  We create a set of the genomes found for the runs.
            NavigableSet<String> genomeIds = new TreeSet<String>();
            Collection<Element> runList = XmlUtils.descendantsOf(runSet, "RUN");
            for (Element runData : runList) {
                String runId = runData.getAttribute("accession");
                String genomeId = this.runMap.get(runId);
                if (genomeId != null)
                    genomeIds.add(runId);
            }
            // There should be exactly one genome ID in the output set.
            if (genomeIds.size() == 0)
                log.error("No genomes found for runs in {}.", experimentId);
            else if (genomeIds.size() > 1)
                log.error("Genome association for runs in {} is ambiguous.", experimentId);
            else {
                // Here we have output to write.
                this.expCount++;
                String genomeId = genomeIds.first();
                writer.println(genomeId + "\t" + this.nameMap.getOrDefault(genomeId, "unknown")
                        + "\t" + experimentId);
            }
        }
    }

}
