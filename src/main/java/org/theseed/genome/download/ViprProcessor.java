/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.apache.commons.io.FileUtils;
import org.theseed.basic.BaseProcessor;
import org.theseed.genome.ViprGenome;

/**
 * This class reads a GFF file and a genome FASTA file downloaded from ViPR and converts them into a GenomeDirectory; that is,
 * a directory of GTOs.
 *
 * The positional parameters are the name of the GFF file, the name of the FASTA file, and the name of the output directory.
 *
 * The command-line options are as follows.
 *
 * -h		display usage information
 * -v		display more progress information on the log
 *
 * --clear		erase the output directory before proceeding
 *
 *
 * @author Bruce Parrello
 *
 */
public class ViprProcessor extends BaseProcessor {

    // COMMAND-LINE OPTIONS

    /** TRUE to clear the output directory during initialization */
    @Option(name = "--clear", usage = "erase output directory before starting")
    private boolean clearOutput;

    /** name of the input GFF file containing the protein definitions */
    @Argument(index = 0, metaVar = "viprData.gff", usage = "protein GFF file downloaded from ViPR", required = true)
    private File gffFile;

    /** name of the input FASTA file containing the virus DNA */
    @Argument(index = 1, metaVar = "viprData.fasta", usage = "genome FASTA file downloaded from ViPR", required = true)
    private File fastaFile;

    /** name of the output directory */
    @Argument(index = 2, metaVar = "outDir", usage = "output directory", required = true)
    private File outDir;

    @Override
    protected void setDefaults() {
        this.clearOutput = false;
    }

    @Override
    protected boolean validateParms() throws IOException {
        // Verify that the input files exist.
        if (! this.gffFile.canRead())
            throw new FileNotFoundException("GFF file " + this.gffFile + " is not found or not readable.");
        if (! this.fastaFile.canRead())
            throw new FileNotFoundException("FASTA file " + this.fastaFile + " is not found or not readable.");
        // Validate the output directory.
        if (! this.outDir.exists()) {
            // Here we must create it.
            log.info("Creating output directory {}.", this.outDir);
            if (! this.outDir.mkdirs())
                throw new IOException("Could not create output directory " + this.outDir + ".");
        } else if (! this.outDir.isDirectory()) {
            throw new FileNotFoundException("Output directory " + this.outDir + " is invalid.");
        } else if (this.clearOutput) {
            // Here we have to erase the directory before we put new stuff in.
            log.info("Clearing output directory {}.", this.outDir);
            FileUtils.cleanDirectory(this.outDir);
        }
        return true;
    }

    @Override
    public void runCommand() throws Exception {
        // Create the genome builder.
        ViprGenome.Builder loader = new ViprGenome.Builder();
        // Load the genomes.
        Collection<ViprGenome> viruses = loader.Load(gffFile, fastaFile);
        // Write the genomes to the output directory.
        for (ViprGenome virus : viruses) {
            File virusFile = new File(this.outDir, virus.getId() + ".gto");
            log.info("Writing {} to {}.", virus.getSourceId(), virusFile);
            virus.save(virusFile);
        }
        log.info("All done. {} viruses output.", viruses.size());
    }

}
