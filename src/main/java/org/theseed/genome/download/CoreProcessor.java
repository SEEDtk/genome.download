/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Genome;
import org.theseed.genome.core.CoreGenome;
import org.theseed.genome.core.OrganismDirectories;
import org.theseed.p3api.Connection;
import org.theseed.utils.BaseProcessor;

/**
 * This command reads genomes from a SEED organism directory.  The positional parameters are the name of the input directory
 * and the name of the output directory.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	write more detailed progress messages to the log
 *
 * --clear	erase the output directory before processing
 *
 * @author Bruce Parrello
 *
 */
public class CoreProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(CoreProcessor.class);

    // COMMAND-LINE OPTIONS

    /** erase the output before starting */
    @Option(name = "--clear", usage = "if specified, the output directory will be erased before processing")
    private boolean clearFlag;

    /** only download new genomes. */
    @Option(name = "--missing", usage = "if specified, only new genomes will be downloaded")
    private boolean missingOnly;

    /** input SEED organism directory */
    @Argument(index = 0, metaVar = "FIGdisk/FIG/Data/Organisms", usage = "SEED organism directory", required = true)
    private File inDir;

    /** output directory for the GTOs */
    @Argument(index = 1, metaVar = "outDir", usage = "output directory", required = true)
    private File outDir;

    @Override
    protected void setDefaults() {
        this.clearFlag = false;
        this.missingOnly = false;
    }

    @Override
    protected boolean validateParms() throws IOException {
        // Verify the input directory.
        if (! this.inDir.isDirectory())
            throw new FileNotFoundException("Input directory " + this.inDir + " not found or invalid.");
        // Verify the output directory.
        if (! this.outDir.isDirectory()) {
            log.info("Creating output directory {}.", this.outDir);
            FileUtils.forceMkdir(this.outDir);
        } else if (this.clearFlag) {
            log.info("Erasing output directory {}.", this.outDir);
            FileUtils.cleanDirectory(this.outDir);
        }
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // Get a PATRIC connection.
        Connection p3 = new Connection();
        // Loop through the genome subdirectories.
        OrganismDirectories orgDirs = new OrganismDirectories(this.inDir);
        log.info("{} genomes found in {}.", orgDirs.size(), this.inDir);
        for (String genomeId : orgDirs) {
            File outFile = new File(this.outDir, genomeId + ".gto");
            if (this.missingOnly && outFile.exists())
                log.info("Skipping {}: already exists.", genomeId);
            else {
                Genome gto = new CoreGenome(p3, new File(this.inDir, genomeId));
                log.info("Writing {} to {}.", gto, outFile);
                gto.save(outFile);
            }
        }
        log.info("All done.");
    }

}
