package org.theseed.genome.download;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.kohsuke.args4j.Option;
import org.theseed.basic.ParseFailureException;
import org.theseed.genome.Contig;
import org.theseed.genome.Genome;
import org.theseed.genome.iterator.BaseGenomeProcessor;
import org.theseed.p3api.P3CursorConnection;
import org.theseed.p3api.SolrFilter;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This command reads genomes from a genome source and attempts to find sequence-identical genomes in the BV-BRC database. A report of the genomes found is written to
 * the standard output.
 * 
 * The normal way to do this would be to use MD5s; however, the sequence MD5s are missing from most BV-BRC genomes. Therefore, we search for genomes with the same name and
 * compare the contigs directly. This is not a very good solution, but it is the best we can do with the data available.
 * 
 * The positional parameter is the genome source file or directory.
 * 
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -t	genome source type (default DIR)
 * -o   output file for the report (if not the standard output)
 * 
 * --max    maximum number of contigs to retrieve from the BV-BRC database (default 10000)
 *
 * @author Bruce Parrello
 * 
 */
public class Md5SurveyProcessor extends BaseGenomeProcessor {

    // FIELDS
    /** logging facility */
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Md5SurveyProcessor.class);
    /** connection to the BV-BRC database */
    private P3CursorConnection p3;
    /** output header line */
    private static final String HEADER = "genome_id\tgenome_name\tdomain\tbvbrc_genome_id";

    // COMMAND-LINE OPTIONS

    /** output file for the report */
    @Option(name = "-o", aliases = { "--output" }, metaVar = "report.tbl", usage = "output file for the report (if not the standard output)")
    private File outFile;

    /** maximum number of contigs to retrieve from the BV-BRC database */
    @Option(name = "--max", metaVar = "1000", usage = "maximum number of contigs to retrieve from the BV-BRC database")
    private int maxContigs;
    
    @Override
    protected void setSourceDefaults() {
        this.outFile = null;
        this.maxContigs = 10000;
    }
    @Override
    protected void validateSourceParms() throws IOException, ParseFailureException {
        if (this.outFile != null) {
            // Test the output file to make sure it works.
            try (PrintWriter writer = new PrintWriter(this.outFile)) {
                writer.println(HEADER);
            }
        }
    }
    @Override
    protected void runCommand() throws Exception {
        // Connect to the BV-BRC database.
        this.p3 = new P3CursorConnection();
        // Open the output file and write the header.
        try (PrintWriter writer = this.openWriter(this.outFile)) {
            writer.println(HEADER);
            // Set up some counters.
            int inGenomeCount = 0;
            int outGenomeCount = 0;
            int noMatchCount = 0;
            // Loop through the genomes.
            for (String genomeId : this.getGenomeIds()) {
                Genome genome = this.getSource().getGenome(genomeId);
                log.info("Processing genome {}.", genome);
                inGenomeCount++;
                // Loop through the contigs and get their MD5s.
                Collection<Contig> inContigs = genome.getContigs();
                // Create a sorted list of the contig sequences. This will be used to compare with the BV-BRC genomes.
                List<String> contigs = inContigs.stream().map(x -> x.getSequence().toLowerCase()).sorted().toList();
                int contigCount = contigs.size();
                // Save the genome name and domain for the report. We also use the name to search the BV-BRC contig table.
                String domain = genome.getDomain();
                String genomeName = genome.getName();
                // Ask the BV-BRC for genomes with this name. This will give us a list of candidate genomes to check for sequence identity.
                // To avoid overloading memory, we cap out at 10,000 contigs. In most cases, we will get at most 200. The problem occurs
                // when the genome name is an unqualified species name, which can have thousands of matches. In that case, we will not find any sequence-identical genomes, 
                // but we will not crash either.
                List<JsonObject> bvbrcContigs = this.p3.getRecords("contig", this.maxContigs, "genome_id,sequence", SolrFilter.EQ("genome_name", genomeName));
                if (bvbrcContigs.isEmpty()) {
                    log.info("No matching contigs found for {}.", genome);
                    // Write a blank line for this genome.
                    writer.println(genomeId + "\t" + genomeName + "\t" + domain + "\t");
                    noMatchCount++;
                } else {
                    if (bvbrcContigs.size() >= this.maxContigs)
                        log.warn("Maximum number of contigs reached for {}. Some matches may be missing.", genome);
                    // We collate the contigs by genome ID. Any one with the same number of matches as the input genome's
                    // contig count is a candidate for being sequence-identical.
                    Map<String, List<String>> genomeContigs = new TreeMap<>();
                    for (JsonObject bvbrcContig : bvbrcContigs) {
                        String bvbrcGenomeId = (String) bvbrcContig.get("genome_id");
                        genomeContigs.computeIfAbsent(bvbrcGenomeId, k -> new ArrayList<>()).add(((String) bvbrcContig.get("sequence")).toLowerCase());
                    }
                    // Now examine each genome found and verify the ones that have enough matches. Note that in all probability, there will be exactly
                    // one genome in the count map, but there are cases where we have duplicate genomes, and other horrible situations where the SOLR
                    // equality operator gets us millions of useless genomes because it matches substrings.
                    int outCount = 0;
                    for (var myGenomeData : genomeContigs.entrySet()) {
                        String myGenomeId = myGenomeData.getKey();
                        List<String> myContigs = myGenomeData.getValue();
                        int myContigCount = myContigs.size();
                        if (myContigCount == contigCount) {
                            // This is a candidate. Now we need to match the sequences.
                            Collections.sort(myContigs);
                            if (myContigs.equals(contigs)) {
                                // We have a match. Write it to the report.
                                log.info("Found sequence-identical genome {} for {}.", myGenomeId, genome);
                                writer.println(genomeId + "\t" + genomeName + "\t" + domain + "\t" + myGenomeId);
                                outCount++;
                                outGenomeCount++;
                            }
                        }
                    }
                    if (outCount == 0) {
                        log.info("No sequence-identical genomes found for {}.", genome);
                        writer.println(genomeId + "\t" + genomeName + "\t" + domain + "\t");
                        noMatchCount++;
                    }
                }
            }
            log.info("{} genomes processed. {} sequence-identical genomes found. {} genomes had no matches.", inGenomeCount, outGenomeCount, noMatchCount);
        }
    }

}
