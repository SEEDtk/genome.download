package org.theseed.genome.download;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.kohsuke.args4j.Option;
import org.theseed.basic.ParseFailureException;
import org.theseed.counters.CountMap;
import org.theseed.genome.Contig;
import org.theseed.genome.Genome;
import org.theseed.genome.iterator.BaseGenomeProcessor;
import org.theseed.p3api.P3CursorConnection;
import org.theseed.p3api.SolrFilter;
import org.theseed.sequence.MD5Hex;

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
 * @author Bruce Parrello
 * 
 */
public class Md5SurveyProcessor extends BaseGenomeProcessor {

    // FIELDS
    /** logging facility */
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Md5SurveyProcessor.class);
    /** connection to the BV-BRC database */
    private P3CursorConnection p3;
    /** MD5 computation helper */
    private MD5Hex md5Computer;
    /** output header line */
    private static final String HEADER = "genome_id\tgenome_name\tdomain\tbvbrc_genome_id\tbvbrc_genome_name";

    // COMMAND-LINE OPTIONS

    /** output file for the report */
    @Option(name = "-o", aliases = { "--output" }, metaVar = "report.tbl", usage = "output file for the report (if not the standard output)")
    private File outFile;
    
    @Override
    protected void setSourceDefaults() {
        this.outFile = null;
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
        // Create the MD5 computer.
        this.md5Computer = new MD5Hex();
        // Open the output file and write the header.
        try (PrintWriter writer = this.openWriter(this.outFile)) {
            writer.println(HEADER);
            // Loop through the genomes.
            for (String genomeId : this.getGenomeIds()) {
                Genome genome = this.getSource().getGenome(genomeId);
                log.info("Processing genome {}.", genome);
                // Loop through the contigs and get their MD5s.
                Collection<Contig> contigs = genome.getContigs();
                int contigCount = contigs.size();
                String domain = genome.getDomain();
                String genomeName = genome.getName();
                List<String> md5s = new ArrayList<>(contigCount);
                for (Contig contig : contigs) {
                    String md5 = this.md5Computer.sequenceMD5(contig.getSequence());
                    md5s.add(md5);
                }
                // Ask the BV-BRC for genomes with these MD5s.
                List<JsonObject> bvbrcContigs = this.p3.getRecords("contig", md5s.size(), 2000, "sequence_md5", md5s, "genome_id,sequence_md5");
                if (bvbrcContigs.isEmpty()) {
                    log.info("No matching contigs found for {}.", genome);
                    // Write a blank line for this genome.
                    writer.println(genomeId + "\t" + genomeName + "\t" + domain + "\t");
                } else {
                    // We count the number of contigs for each genome ID. Any one with the same number of matches as the input genome's
                    // contig count is a candidate for being sequence-identical.
                    CountMap<String> genomeCounts = new CountMap<>();
                    for (JsonObject bvbrcContig : bvbrcContigs) {
                        String bvbrcGenomeId = (String) bvbrcContig.get("genome_id");
                        genomeCounts.count(bvbrcGenomeId);
                    }
                    // Now examine each genome found and verify the ones that have enough matches. Note that in all probability, there will be exactly
                    // one genome in the count map, but there are rare cases where we have duplicate genomes.
                    int outCount = 0;
                    for (String bvbrcGenomeId : genomeCounts.keys()) {
                        int count = genomeCounts.getCount(bvbrcGenomeId);
                        if (count == contigCount) {
                            // This is a candidate. Insure it has no extra contigs. Since we can't trust the contig count, we have to retrieve all the contigs for the
                            // genome.
                            List<JsonObject> bvbrcGenomeContigs = this.p3.getRecords("contig", P3CursorConnection.MAX_LIMIT, "genome_id,genome_name,sequence_md5",
                                    SolrFilter.EQ("genome_id", bvbrcGenomeId));
                            if (bvbrcGenomeContigs.size() == contigCount) {
                                // This genome is sequence-identical. Get its name and write the output line. Note we are assured at least
                                // one contig exists, so we can get the name from the first one.
                                String bvbrcGenomeName = (String) bvbrcGenomeContigs.get(0).get("genome_name");
                                log.info("Found sequence-identical genome {} in BV-BRC.", bvbrcGenomeId);
                                writer.println(genomeId + "\t" + genomeName + "\t" + domain + "\t" + bvbrcGenomeId + "\t" + bvbrcGenomeName);
                                outCount++;
                            }
                        }
                    }
                    if (outCount == 0) {
                        log.info("No sequence-identical genomes found for {}.", genome);
                        writer.println(genomeId + "\t" + genomeName + "\t" + domain + "\t");
                    }
                }
            }
        }
    }

}
