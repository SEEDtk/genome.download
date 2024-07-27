/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.IOException;
import java.util.Collection;


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
import org.theseed.sequence.Sequence;
import org.theseed.utils.BaseInputProcessor;

/**
 * This command takes the output from the SraMapProcessor and creates a pseudo-sample directory containing FASTA files
 * for the named genomes.  These can be used to compare the performance of contig sets to read sets.
 *
 * The standard input should contain the SraMapProcessor output.  The positional parameter is the name of the output
 * directory.
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file containing SRA mappings (if not STDIN)
 *
 * --clear		erase the output directory before processing
 * --missing	if specified, existing output subdirectories will not be re-created
 *
 * @author Bruce Parrello
 *
 */
public class SraFastaProcessor extends BaseInputProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SraFastaProcessor.class);
    /** column index of sample ID column */
    private int sampleIdColIdx;
    /** column index of genome ID column */
    private int genomeIdColIdx;
    /** genome sequence reader */
    private SequenceReader seqReader;
    /** connection to PATRIC */
    private P3Connection p3;

    // COMMAND-LINE OPTIONS

    /** if specified, the output directory will be erased before processing */
    @Option(name = "--clear", usage = "if specified, the output directory will be erased before processing")
    private boolean clearFlag;

    /** if specified, existing output subdirectories will be skipped */
    @Option(name = "--missing", usage = "f specified, existing output subdirectories will be skipped")
    private boolean missingFlag;

    /** output directory name */
    @Argument(index = 0, metaVar = "outDir", usage = "name of the output directory", required = true)
    private File outDir;

    @Override
    protected void setReaderDefaults() {
        this.clearFlag = false;
        this.missingFlag = false;
    }

    @Override
    protected void validateReaderParms() throws IOException, ParseFailureException {
        // Set up the output directory.
        if (! this.outDir.isDirectory()) {
            log.info("Creating output directory {}.", this.outDir);
            FileUtils.forceMkdir(this.outDir);
        } else if (this.clearFlag) {
            log.info("Erasing output directory {}.", this.outDir);
            FileUtils.cleanDirectory(this.outDir);
        } else
            log.info("Samples will be created in output directory {}.", this.outDir);
        // Connect to PATRIC to read the sequences.
        log.info("Connecting to PATRIC.");
        this.p3 = new P3Connection();
        this.seqReader = SequenceReader.Type.CONTIGS.create(this.p3);
    }

    @Override
    protected void validateReaderInput(TabbedLineReader reader) throws IOException {
        // Get the indices of the important columns.
        this.sampleIdColIdx = reader.findField("sample_id");
        this.genomeIdColIdx = reader.findField("genome_id");
    }

    @Override
    protected void runReader(TabbedLineReader reader) throws Exception {
        // We loop through the input file.  For each line, we create a directory with the sample ID name,
        // and dump the contigs into it in FASTA format.
        int inCount = 0;
        int skipCount = 0;
        for (var line : reader) {
            String sampleId = line.get(this.sampleIdColIdx);
            File sampleDir = new File(this.outDir, sampleId);
            inCount++;
            if (this.missingFlag && sampleDir.isDirectory()) {
                skipCount++;
                log.info("Sample #{} {} already exists: skipping.", inCount, sampleId);
            } else {
                if (! sampleDir.isDirectory()) {
                    log.info("Creating sample output directory {}.", sampleDir);
                    FileUtils.forceMkdir(sampleDir);
                }
                File sampleFile = new File(sampleDir, "contigs.fasta");
                try (FastaOutputStream outStream = new FastaOutputStream(sampleFile)) {
                    String genomeId = line.get(this.genomeIdColIdx);
                    log.info("Reading sample #{} {} genome {}.", inCount, sampleId, genomeId);
                    Collection<Sequence> sequences = this.seqReader.getSequences(genomeId);
                    outStream.write(sequences);
                    log.info("{} sequences written for genome {} to {}.", sequences.size(), genomeId, sampleFile);
                }
            }
        }
        log.info("{} samples found. {} skipped.", inCount, skipCount);
    }

}
