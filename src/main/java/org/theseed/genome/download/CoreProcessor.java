/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Argument;
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
 * @author Bruce Parrello
 *
 */
public class CoreProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(CoreProcessor.class);

    // COMMAND-LINE OPTIONS

    /** input SEED organism directory */
    @Argument(index = 0, metaVar = "FIGdisk/FIG/Data/Organisms", usage = "SEED organism directory", required = true)
    private File inDir;

    /** output directory for the GTOs */
    @Argument(index = 1, metaVar = "outDir", usage = "output directory", required = true)
    private File outDir;

    @Override
    protected void setDefaults() {
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
            Genome gto = new CoreGenome(p3, new File(this.inDir, genomeId));
            File outFile = new File(this.outDir, gto.getId() + ".gto");
            log.info("Writing {} to {}.", gto, outFile);
            gto.update(outFile);
        }
        log.info("All done.");
    }

}
