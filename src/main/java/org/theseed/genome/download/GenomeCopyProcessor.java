/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Set;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Genome;
import org.theseed.genome.iterator.GenomeSource;
import org.theseed.genome.iterator.GenomeTargetType;
import org.theseed.genome.iterator.IGenomeTarget;
import org.theseed.io.TabbedLineReader;
import org.theseed.utils.BaseProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * Copy a set of genomes to a genome directory.  Note that if the input is PATRIC, the output directory will
 * contain full genomes and the job is not restartable, so this is a bad idea for a large genome set.
 *
 * The positional parameters are the name of the input genome source and the name of the output directory.
 * If the output directory already exists, it will be updated.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --clear		erase the output directory before starting
 * --source		type of input (master directory, GTO directory, PATRIC ID file); default is GTO directory
 * --target		type of output (master directory, GTO directory); default is GTO directory
 * --missing	only copy a genome if it is not already in the output directory
 * --filter		if specified, the name of a tab-delimited file (with headers) containing genome IDs in the
 * 				first column; only the specified genomes will be copied
 *
 * @author Bruce Parrello
 *
 */
public class GenomeCopyProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(GenomeCopyProcessor.class);
    /** input genomes */
    private GenomeSource genomes;
    /** output master directory */
    private IGenomeTarget targetDir;
    /** filter set */
    private Set<String> filterSet;

    /** TRUE to erase any old genomes */
    @Option(name = "--clear", usage = "if specified, the output directory will be erased before starting")
    private boolean clearFlag;

    /** TRUE to only copy new genomes */
    @Option(name = "--missing", usage = "if specified, input genomes not overwrite existing genomes")
    private boolean missingFlag;

    /** type of input */
    @Option(name = "--source", usage = "type of genome input (master directory, normal, PATRIC ID file)")
    private GenomeSource.Type inType;

    /** type of output */
    @Option(name = "--target", usage = "type of genome output (master directory, normal)")
    private GenomeTargetType outType;

    /** filter file */
    @Option(name = "--filter", metaVar = "include.tbl", usage = "file containing IDs of genomes to copy")
    private File filterFile;

    /** input genome file or directory */
    @Argument(index = 0, metaVar = "inDir", usage = "input genomes (file or directory)", required = true)
    private File inDir;

    /** output master directory */
    @Argument(index = 1, metaVar = "outDir", usage = "output genome master directory", required = true)
    private File outDir;

    @Override
    protected void setDefaults() {
        this.inType = GenomeSource.Type.DIR;
        this.outType = GenomeTargetType.DIR;
        this.clearFlag = false;
        this.missingFlag = false;
        this.filterFile = null;
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // Create the source stream.
        this.genomes = this.inType.create(this.inDir);
        // Connect to the output directory.
        this.targetDir = this.outType.create(this.outDir, this.clearFlag);
        log.info("Copying from {} to {}.", this.inDir, this.outDir);
        // Create the filter if needed.
        if (this.filterFile == null)
            this.filterSet = this.genomes.getIDs();
        else {
            this.filterSet = TabbedLineReader.readSet(this.filterFile, "1");
            log.info("{} genome IDs read from filter file {}.", this.filterSet.size(),
                    this.filterFile);
            this.filterSet.retainAll(this.genomes.getIDs());
        }
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        long count = 0;
        long total = this.filterSet.size();
        long start = System.currentTimeMillis();
        log.info("{} genomes will be copied.", this.filterSet.size());
        for (String genomeId : this.filterSet) {
            // Verify that we should copy this genome.
            if (this.missingFlag && this.targetDir.contains(genomeId))
                log.info("Genome {} already in target-- skipped.", genomeId);
            else {
                // Try to get the genome.
                Genome genome = this.genomes.getGenome(genomeId);
                // Copy it.
                if (genome != null)
                    this.targetDir.add(genome);
            }
            count++;
            Duration speed = Duration.ofMillis(System.currentTimeMillis() - start).dividedBy(count);
            log.info("{} of {} processed; {} per genome.", count, total, speed.toString());
        }
    }

}
