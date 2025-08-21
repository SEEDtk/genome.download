/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Argument;
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
 * This command downloads genome FASTA files from PATRIC.  It reads a file of genome IDs on the standard input and
 * downloads the FASTA files to a specified output directory.  The FASTA files can either be contig-oriented (DNA
 * only) or feature-oriented (DNA or proteins).
 *
 * The positional parameter is the name of the output directory.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file containing genome IDs (if not STDIN)
 * -c	index (1-based) or name of column containing genome IDs (default "1")
 *
 * --clear		erase the output directory before processing
 * --missing	do not overwrite existing files
 * --type		type of FASTA file to output (default CONTIGS)
 *
 * @author Bruce Parrello
 *
 */
public class FastaDirProcessor extends BaseInputProcessor {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(FastaDirProcessor.class);
    /** sequence reader for downloading the genomes */
    private SequenceReader seqReader;
    /** set of genome IDs to process */
    private Set<String> genomeIDs;
    /** filename extension to use (with period) */
    private String fileExt;
    /** set of genomes already in the output directory */
    private Set<String> skipGenomes;

    // COMMAND-LINE OPTIONS

    /** index (1-based) or name of input column containing genome IDs */
    @Option(name = "--col", aliases = { "-c" }, metaVar = "genome_id", usage = "index (1-based) or name of input column containing genome IDs")
    private String genomeCol;

    /** if specified, the output directory will be erased prior to processing */
    @Option(name = "--clear", usage = "if specified, the output directory will be erased before processing")
    private boolean clearFlag;

    /** if specified, existing files will not be overwritten */
    @Option(name = "--missing", usage = "if specified, existing FASTA files will not be overwritten")
    private boolean missingOnly;

    /** type of FASTA file to download */
    @Option(name = "--type", usage = "type of FASTA file to download")
    private SequenceReader.Type fileType;

    /** name of the output directory */
    @Argument(index = 0, metaVar = "outDir", usage = "output directory for FASTA files", required = true)
    private File outDir;

    @Override
    protected void setReaderDefaults() {
        this.fileType = SequenceReader.Type.CONTIGS;
        this.clearFlag = false;
        this.missingOnly = false;
        this.genomeCol = "1";
    }

    @Override
    protected void validateReaderParms() throws IOException, ParseFailureException {
        // Prepare the output directory.
        if (! this.outDir.isDirectory()) {
            log.info("Creating output directory {}.", this.outDir);
            FileUtils.forceMkdir(this.outDir);
        } else if (this.clearFlag) {
            log.info("Erasing output directory {}.", this.outDir);
            FileUtils.cleanDirectory(this.outDir);
        } else
            log.info("Output will be written to {}.", this.outDir);
        // Create the sequence reader for downloading.
        log.info("Setting up to output fasta files of type {}.", this.fileType);
        P3Connection p3 = new P3Connection();
        this.seqReader = this.fileType.create(p3);
        // Set up the filename helper.
        this.fileExt = "." + this.seqReader.getExtension();
        // If this is missing-only, get the genomes already in the output directory.
        if (! this.missingOnly)
            this.skipGenomes = Collections.emptySet();
        else {
            // This pattern will match existing files of the correct type.
            Pattern filePattern = Pattern.compile("(\\d+\\.\\d+)\\." + this.seqReader.getExtension(), Pattern.CASE_INSENSITIVE);
            File[] outFiles = this.outDir.listFiles();
            this.skipGenomes = new HashSet<String>(outFiles.length * 4 / 3 + 1);
            for (File outFile : outFiles) {
                if (outFile.isFile()) {
                    Matcher m = filePattern.matcher(outFile.getName());
                    if (m.matches())
                        this.skipGenomes.add(m.group(1));
                }
            }
            log.info("{} genomes already present in {} and will be skipped.", this.skipGenomes.size(), this.outDir);
        }
    }

    @Override
    protected void validateReaderInput(TabbedLineReader reader) throws IOException {
        // Here we read in the list of genomes to process.  We use a tree set so they are processed in order,
        // giving the user an idea of progress.
        this.genomeIDs = new TreeSet<String>();
        int colIdx = reader.findField(this.genomeCol);
        log.info("Reading genome IDs from input.");
        int skipCount = 0;
        for (var line : reader) {
            String genomeID = line.get(colIdx);
            // Here we skip genomes if "--missing" is set.
            if (this.skipGenomes.contains(genomeID))
                skipCount++;
            else
                this.genomeIDs.add(genomeID);
        }
        log.info("{} genomes will be output. {} skipped.", this.genomeIDs.size(), skipCount);
    }

    @Override
    protected void runReader(TabbedLineReader reader) throws Exception {
        // Loop through the genome IDs.
        int gCount = 0;
        int errorCount = 0;
        final int gTotal = this.genomeIDs.size();
        for (String genomeID : this.genomeIDs) {
            gCount++;
            // Create the output file name.
            File outFile = new File(this.outDir, genomeID + this.fileExt);
            log.info("Writing genome {} of {} to {}.", gCount, gTotal, outFile);
            var sequences = this.seqReader.getSequences(genomeID);
            if (sequences.isEmpty()) {
                log.warn("No sequences found for genome {}.", genomeID);
                errorCount++;
            } else {
                // Here we can output the genome.
                try (FastaOutputStream outStream = new FastaOutputStream(outFile)) {
                    outStream.write(sequences);
                }
            }
        }
        log.info("{} genomes processed, {} failed.", gCount, errorCount);
    }

}
