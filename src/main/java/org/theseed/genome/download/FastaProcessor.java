/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.P3Connection;
import org.theseed.p3api.SequenceReader;
import org.theseed.sequence.FastaOutputStream;
import org.theseed.utils.BaseInputProcessor;

/**
 * This command downloads genome FASTA files from PATRIC into a single file.  It reads a file of genome IDs
 * on the standard input and downloads the FASTA files to the standard output.  The FASTA files can either
 * be contig-oriented (DNA only) or feature-oriented (DNA or proteins).
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file containing genome IDs (if not STDIN)
 * -o	output file (if not STDOUT)
 * -c	index (1-based) or name of column containing genome IDs (default "1")
 *
 * --clear		erase the output directory before processing
 * --missing	do not overwrite existing files
 * --type		type of FASTA file to output (default CONTIGS)
 *
 * @author Bruce Parrello
 *
 */
public class FastaProcessor extends BaseInputProcessor {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(FastaProcessor.class);
    /** sequence reader for downloading the genomes */
    private SequenceReader seqReader;
    /** set of genome IDs to process */
    private Set<String> genomeIDs;

    // COMMAND-LINE OPTIONS

    /** name of the output file (if not STDOUT) */
    @Option(name = "--output", aliases = { "-o" }, metaVar = "outFile.fasta", usage = "name of the output file (if not STDOUT)")
    private File outFile;

    /** index (1-based) or name of input column containing genome IDs */
    @Option(name = "--col", aliases = { "-c" }, metaVar = "genome_id", usage = "index (1-based) or name of input column containing genome IDs")
    private String genomeCol;

    /** type of FASTA file to download */
    @Option(name = "--type", usage = "type of FASTA file to download")
    private SequenceReader.Type fileType;

    @Override
    protected void setReaderDefaults() {
        this.fileType = SequenceReader.Type.CONTIGS;
        this.genomeCol = "1";
        this.outFile = null;
    }

    @Override
    protected void validateReaderParms() throws IOException, ParseFailureException {
        // Create the sequence reader for downloading.
        log.info("Setting up to output fasta files of type {}.", this.fileType);
        P3Connection p3 = new P3Connection();
        this.seqReader = this.fileType.create(p3);
    }

    @Override
    protected void validateReaderInput(TabbedLineReader reader) throws IOException {
        // Here we read in the list of genomes to process.  We use a tree set so they are processed in order,
        // giving the user an idea of progress.
        this.genomeIDs = new TreeSet<String>();
        int colIdx = reader.findField(this.genomeCol);
        log.info("Reading genome IDs from input.");
        for (var line : reader) {
            String genomeID = line.get(colIdx);
            this.genomeIDs.add(genomeID);
        }
        log.info("{} genomes will be output.", this.genomeIDs.size());
    }

    @Override
    protected void runReader(TabbedLineReader reader) throws Exception {
        // Open the output stream.
        try (FastaOutputStream outStream = this.openOutput()) {
            // Loop through the genome IDs.
            int gCount = 0;
            int errorCount = 0;
            final int gTotal = this.genomeIDs.size();
            for (String genomeID : this.genomeIDs) {
                gCount++;
                log.info("Writing genome {} of {}: {}.", gCount, gTotal, genomeID);
                var sequences = this.seqReader.getSequences(genomeID);
                if (sequences.isEmpty()) {
                    log.warn("No sequences found for genome {}.", genomeID);
                    errorCount++;
                } else {
                    // Here we can output the genome.
                    outStream.write(sequences);
                }
            }
            log.info("{} genomes processed, {} failed.", gCount, errorCount);
        }
    }

    /**
     * @return the FASTA output stream to use
     *
     * @throws IOException
     */
    private FastaOutputStream openOutput() throws IOException {
        FastaOutputStream retVal;
        if (this.outFile == null) {
            log.info("FASTA will be written to standard output");
            retVal = new FastaOutputStream(System.out);
        } else {
            log.info("FASTA will be written to {}.", this.outFile);
            retVal = new FastaOutputStream(this.outFile);
        }
        return retVal;
    }

}
