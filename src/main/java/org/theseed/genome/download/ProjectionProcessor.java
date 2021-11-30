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
import org.theseed.genome.GenomeDirectory;
import org.theseed.subsystems.SubsystemProjector;
import org.theseed.utils.BaseProcessor;

/**
 * This command projects subsystems onto existing GTOs.  The positional parameters are the name of the subsystem projection
 * file, the name of the directory containing the GTOs, and the name of an output directory.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more detailed progress messages
 *
 * --clear		erase the output directory before processing
 *
 * @author Bruce Parrello
 */
public class ProjectionProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(ProjectionProcessor.class);
    /** subsystem projector */
    private SubsystemProjector projector;

    // COMMAND-LINE OPTIONS

    @Option(name = "--clear", usage = "clear output directory before processing")
    private boolean clearFlag;

    /** projector file */
    @Argument(index = 0, metaVar = "variants.tbl", usage = "subsystem projector file")
    private File projectorFile;

    /** input directory */
    @Argument(index = 1, metaVar = "gtoDir", usage = "genome directory")
    private File gtoDir;

    /** output directory */
    @Argument(index = 2, metaVar = "outDir", usage = "output directory")
    private File outDir;

    @Override
    protected void setDefaults() {
    }

    @Override
    protected boolean validateParms() throws IOException {
        // Verify the input files.
        if (! this.projectorFile.canRead())
            throw new FileNotFoundException("Projector file " + this.projectorFile + " not found or unreadable.");
        if (! this.gtoDir.isDirectory())
            throw new FileNotFoundException("Input directory " + this.gtoDir + " not found or invalid.");
        // Verify the output directory.
        if (! this.outDir.exists()) {
            log.info("Creating output directory {}.", this.outDir);
            FileUtils.forceMkdir(this.outDir);
        } else if (! this.outDir.isDirectory())
            throw new FileNotFoundException("Output directory " + this.outDir + " is invalid.");
        else {
            if (this.clearFlag) {
                log.info("Erasing output directory {}.", this.outDir);
                FileUtils.cleanDirectory(this.outDir);
            } else
                log.info("Output will be to {}.", this.outDir);
        }
        // Read the projector file.
        log.info("Reading subsystem projector from {}.", this.projectorFile);
        this.projector = SubsystemProjector.load(this.projectorFile);
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // Loop through the genomes, updating them.
        log.info("Scanning input directory {}.", this.gtoDir);
        GenomeDirectory genomes = new GenomeDirectory(this.gtoDir);
        log.info("{} genomes found in directory.", genomes.size());
        for (Genome genome : genomes) {
            log.info("Scanning {}.", genome);
            this.projector.project(genome);
            File outFile = new File(this.outDir, genome.getId() + ".gto");
            log.info("Saving genome to {}.", outFile);
            genome.save(outFile);
        }
    }

}
