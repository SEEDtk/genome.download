/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.genome.Genome;
import org.theseed.genome.SubsystemRow;
import org.theseed.subsystems.VariantId;
import org.theseed.subsystems.core.SubsystemRuleProjector;

/**
 * This is a simple command that projects subsystems onto a single GTO. A new GTO is created with the projected subsystems.
 * The positional parameter is the name of the subsystem projection file. The input GTO file should be on the standard
 * input, and the output GTO file will be on the standard output.
 *
 * The positional parameters are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input GTO file (if not STDIN)
 * -o	output GTO file (if not STDOUT)
 *
 * --active		if specified, only active subsystems will be projected
 * --patric		if specified, variant codes will be translated to "active" and "inactive"
 *
 *
 * @author Bruce Parrello
 *
 */
public class Project1Processor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(Project1Processor.class);
    /** subsystem projector */
    private SubsystemRuleProjector projector;
    /** target GTO */
    private Genome genome;

    /** input file (if not STDIN) */
    @Option(name = "--input", aliases = { "-i" }, metaVar = "input.gto", usage = "input GTO file (if not STDIN)")
    private File inFile;

    /** output file (if not STDOUT) */
    @Option(name = "--output", aliases = { "-o" }, metaVar = "output.gto", usage = "output GTO file (if not STDIN)")
    private File outFile;

    /** TRUE if only active subsystems should be projected */
    @Option(name = "--active", usage = "if specified, only active subsystems will be projected")
    private boolean activeFlag;

    /** TRUE if variant codes should be normalized */
    @Option(name = "--patric", usage = "if specified, all variant codes will be normalized to \"active\" and \"inactive\"")
    private boolean patricFlag;

    /** file name for the projector */
    @Argument(index = 0, metaVar = "projector.ser", usage = "name of the subsystem projector file")
    private File projectorFile;


    @Override
    protected void setDefaults() {
        this.inFile = null;
        this.outFile = null;
        this.activeFlag = false;
        this.patricFlag = false;
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // Load the subsystem projector.
        if (! this.projectorFile.canRead())
            throw new FileNotFoundException("Subsystem projector file " + this.projectorFile + " is not found or unreadable.");
        log.info("Loading subsystem projector from {}.", this.projectorFile);
        this.projector = SubsystemRuleProjector.load(this.projectorFile);
        // Load the input GTO.
        if (this.inFile == null) {
            // Here we are loading from the standard input.
            this.genome = new Genome(System.in);
            log.info("Input genome {} loaded from standard input.", genome);
        } else if (! this.inFile.canRead())
            throw new FileNotFoundException("Input genome file " + this.inFile + " is not found or unreadable.");
        else {
            // Here we are loading from a file.
            this.genome = new Genome(this.inFile);
            log.info("Input genome {} loaded from {}.", genome, this.inFile);
        }
        // Verify we can write to the output file.
        if (this.outFile != null) {
            try (PrintWriter testStream = new PrintWriter(this.outFile)) {
                testStream.println("{}");
            }
        }
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // Project the subsystems onto the genome.
        log.info("Projecting subsystems.");
        this.projector.project(this.genome, this.activeFlag);
        Collection<SubsystemRow> rows = genome.getSubsystems();
        log.info("{} subsystems found in {}.", rows.size(), genome);
        // If we are in PATRIC mode, fix the variant codes.
        if (this.patricFlag) {
            log.info("Normalizing variant codes.");
            int rowCount = 0;
            int changeCount = 0;
            for (SubsystemRow row : genome.getSubsystems()) {
                rowCount++;
                String oldCode = row.getVariantCode();
                String newCode = (VariantId.isActive(oldCode) ? "active" : "inactive");
                if (! newCode.equals(oldCode)) {
                    row.setVariantCode(newCode);
                    changeCount++;
                }
            }
            log.info("{} of {} variant codes updated.", changeCount, rowCount);
        }
        // Save the updated GTO.
        log.info("Saving updated genome.");
        if (this.outFile == null) {
            String jsonString = genome.toJsonString();
            System.out.println(jsonString);
            log.info("Genome written to standard output.");
        } else {
            genome.save(this.outFile);
            log.info("Genome saved to {}.", this.outFile);
        }
    }

}
