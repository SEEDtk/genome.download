/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.genome.iterator.GenomeTargetType;
import org.theseed.genome.iterator.IGenomeTarget;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.P3CursorConnection;
import org.theseed.p3api.P3Genome;
import org.theseed.utils.BaseInputProcessor;

/**
 * This command takes a list of genome IDs from the standard input and insures this list of genomes matches what is in
 * a particular genome tagret.  Genomes that are not in the list are deleted, and genomes in the list but not in the
 * target are downloaded from PATRIC.
 *
 * The standard input should contain the genome IDs in a column named "genome_id".  The column name can be changed
 * via a command-line option.  The positional parameter is the name of the genome target directory to update.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file containing the genome IDs (if not STDIN)
 * -c	index (1-based) or name of the input column containing the genome IDs (default "genome_id")
 *
 * --target		type of genome target (default DIR, indicating a normal GTO directory)
 * --detail		detail level for genome download (default FULL)
 *
 * @author Bruce Parrello
 *
 */
public class SynchronizeProcessor extends BaseInputProcessor {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(SynchronizeProcessor.class);
    /** genome target to synchronize */
    private IGenomeTarget target;
    /** set of genome IDs to synchronize with */
    private Set<String> genomeIDs;

    // COMMAND-LINE OPTIONS

    /** input column to use for genoem IDs */
    @Option(name = "--col", aliases = { "-c" }, metaVar = "1", usage = "index (1-based) or name of genome ID input column")
    private String colName;

    /** detail level for downloads from PATRIC */
    @Option(name = "--detail", aliases = { "--level" }, usage = "detail level for genomes downloaded from PATRIC")
    private P3Genome.Details level;

    /** type of genome target */
    @Option(name = "--target", aliases = { "--type" }, usage = "type of genome target being synchronized")
    private GenomeTargetType targetType;

    /** name of the target directory */
    @Argument(index = 0, metaVar = "targetDir", usage = "name of the genome target directory to synchronize")
    private File targetDir;

    @Override
    protected void setReaderDefaults() {
        this.colName = "genome_id";
        this.targetType = GenomeTargetType.DIR;
        this.level = P3Genome.Details.FULL;
    }

    @Override
    protected void validateReaderParms() throws IOException, ParseFailureException {
        // Insure the target directory is valid.
        if (! this.targetDir.isDirectory())
            throw new FileNotFoundException("Target directory " + this.targetDir + " is not found or is not a directory.");
        log.info("Connecting to {}.", this.targetDir);
        this.target = this.targetType.create(targetDir, false);
        if (! this.target.canDelete())
            throw new ParseFailureException("Target type " + this.targetType + " does not support synchronization.");
    }

    @Override
    protected void validateReaderInput(TabbedLineReader reader) throws IOException {
        // Here we verify the input column and read in all the genome IDs.
        int colIdx = reader.findField(this.colName);
        int inCount = 0;
        this.genomeIDs = new HashSet<>(1000);
        for (var line : reader) {
            String genomeID = line.get(colIdx);
            inCount++;
            if (! StringUtils.isBlank(genomeID)) {
                this.genomeIDs.add(genomeID);
                if (log.isInfoEnabled() && inCount % 1000 == 0)
                    log.info("{} lines read and {} genome IDs found.", inCount, this.genomeIDs.size());
            }
        }
        log.info("{} genome IDs read from input.", this.genomeIDs.size());
    }

    @Override
    protected void runReader(TabbedLineReader reader) throws Exception {
        // Connect to PATRIC.
        log.info("Connecting to PATRIC.");
        P3CursorConnection p3 = new P3CursorConnection();
        // First, delete the extras.
        Set<String> targetIDs = this.target.getGenomeIDs();
        log.info("Checking {} genomes to delete obsolete ones.", targetIDs.size());
        int deleteCount = 0;
        for (String targetID : targetIDs) {
            if (! this.genomeIDs.contains(targetID)) {
                log.info("Deleting genome {} from {}.", targetID, this.targetDir);
                this.target.remove(targetID);
                deleteCount++;
            }
        }
        log.info("{} genomes deleted from {}.", deleteCount, this.targetDir);
        // Now add the missing ones.
        log.info("Search {} for missing genomes.", this.targetDir);
        int insCount = 0;
        for (String genomeID : this.genomeIDs) {
            if (! this.target.contains(genomeID)) {
                log.info("Downloading genome {} to {}.", genomeID, this.targetDir);
                P3Genome genome = P3Genome.load(p3, genomeID, this.level);
                this.target.add(genome);
                insCount++;
            }
        }
        log.info("{} genomes added to {}.", insCount, this.targetDir);
    }

}
