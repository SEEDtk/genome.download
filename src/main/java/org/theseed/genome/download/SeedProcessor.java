/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.theseed.genome.core.OrganismDirectories;
import org.theseed.subsystems.SubsystemFilter;
import org.theseed.utils.BaseProcessor;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.Argument;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * This command copies the small subset of SEED files needed for testing the CoreSEED utilities.
 * This includes parts of the Organisms and Subsystems directories.
 *
 * The positional parameters are the names of the input CoreSEED data directory and the name of the
 * output directory.
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	show more detailed progress messages
 *
 * --clear		erase the output directory before beginning (otherwise only missing items will be copied)
 * --win		change directory names that are invalid in Windows
 *
 * @author Bruce Parrello
 *
 */
public class SeedProcessor extends BaseProcessor {

    // FIELDS
    /** input subsystem directory */
    private File subsysIn;
    /** input organism directory */
    private File orgIn;
    /** output subsystem directory */
    private File subsysOut;
    /** output organism directory */
    private File orgOut;
    /** number of genomes copied */
    private int gCopyCount;
    /** number of subsystems copied */
    private int sCopyCount;
    /** number of files copied */
    private int fCopyCount;
    /** list of subsystem file names */
    private static final String[] SUBSYS_FILES = new String[] { "CLASSIFICATION", "EXCHANGABLE", "spreadsheet" };
    /** list of genome file names */
    private static final String[] GENOME_FILES = new String[] {"contigs", "annotations", "GENOME", "TAXONOMY_ID", "TAXONOMY",
            "assigned_functions" };
    /** list of feature directory files */
    private static final String[] FEATURE_FILES = new String[] { "deleted.features", "fasta" };
    /** list of invalid characters */
    private static final String BAD_CHARS = "?\\<>:*";
    /** map of character replacements */
    private static final String[] CHAR_MAP = buildCharMap(BAD_CHARS);

    // COMMAND-LINE OPTIONS

    /** TRUE to clear the output directory first */
    @Option(name = "--clear", usage = "clear output directory before beginning")
    private boolean clearFlag;

    /** TRUE to translate directory names that are invalid in Windows */
    @Option(name = "--win", usage = "change invalid subsystem names")
    private boolean winFlag;

    /** input SEED data directory */
    @Argument(index = 0, metaVar = "/vol/core-seed/FIGdisk/FIG/Data", usage = "input coreSEED directory", required = true)
    private File coreDir;

    /** output directory */
    @Argument(index = 1, metaVar = "CoreCopy", usage = "output directory", required = true)
    private File outDir;


    @Override
    protected void setDefaults() {
        this.clearFlag = false;
        this.sCopyCount = 0;
        this.gCopyCount = 0;
        this.fCopyCount = 0;
    }

    /**
     * This initializes the character translation map.
     *
     * @param badChars	list of characters to translate
     *
     * @return an array that can be used to translate bad characters
     */
    private static String[] buildCharMap(String badChars) {
        String[] retVal = new String[badChars.length()];
        for (int i = 0; i < badChars.length(); i++) {
            int badChar = badChars.charAt(i);
            retVal[i] = String.format("%%%02X", badChar);
        }
        return retVal;
    }

    @Override
    protected boolean validateParms() throws IOException {
        // Insure the input directory is valid.
        if (! this.coreDir.isDirectory())
            throw new FileNotFoundException("Input coreSEED directory " + this.coreDir + " not found or invalid.");
        this.subsysIn = new File(this.coreDir, "Subsystems");
        this.orgIn = new File(this.coreDir, "Organisms");
        if (! this.subsysIn.isDirectory() || ! this.orgIn.isDirectory())
            throw new FileNotFoundException("Input coreSEED directory " + this.coreDir + " is missing Organisms or Subsystems sub-directory.");
        // Set up the output directory.
        if (! this.outDir.isDirectory()) {
            log.info("Creating output directory {}.", this.outDir);
            FileUtils.forceMkdir(this.outDir);
        }
        this.subsysOut = new File(this.outDir, "Subsystems");
        this.orgOut = new File(this.outDir, "Organisms");
        this.setupOutputDir(this.subsysOut);
        this.setupOutputDir(this.orgOut);
        return true;
    }

    /**
     * Insure the specified output directory is valid and, if necessary, clears.
     *
     * @param outSubDir		output directory to check
     *
     * @throws IOException
     */
    private void setupOutputDir(File outSubDir) throws IOException {
        if (! outSubDir.exists()) {
            log.info("Creating output subdirectory {}.", outSubDir);
            FileUtils.forceMkdir(outSubDir);
        } else if (! outSubDir.isDirectory())
            throw new IOException(outSubDir + " must be a subdirectory, but it is a file.");
        else if (this.clearFlag) {
            log.info("Erasing output subdirectory {}.", outSubDir);
            FileUtils.cleanDirectory(outSubDir);
        }
    }

    @Override
    protected void runCommand() throws Exception {
        // Copy the subsystems.
        this.copySubsystems();
        // Copy the genomes.
        this.copyGenomes();
        // Denote we're done.
        log.info("All done.  {} genomes, {} subsystems, and {} total files copied.", this.gCopyCount, this.sCopyCount,
                this.fCopyCount);
    }

    /**
     * Copy the genomes to the output directory.
     *
     * @throws IOException
     */
    private void copyGenomes() throws IOException {
        // Get the list of genome directories.
        OrganismDirectories orgDirs = new OrganismDirectories(this.orgIn);
        log.info("{} genomes found in {}.", orgDirs.size(), this.orgIn);
        for (String genomeId : orgDirs) {
            File genomeIn = new File(this.orgIn, genomeId);
            File genomeOut = new File(this.orgOut, genomeId);
            if (genomeOut.exists())
                log.debug("{} exists:  skipping.", genomeId);
            else {
                // Copy the basic genome files.
                log.debug("Copying genome {}.", genomeId);
                this.dirCopy(genomeIn, genomeOut, GENOME_FILES);
                // Now we need to process each feature directory.
                File featureDirsIn = new File(genomeIn, "Features");
                File featureDirsOut = new File(genomeOut, "Features");
                if (featureDirsIn.isDirectory()) {
                    File[] fTypeDirs = featureDirsIn.listFiles(File::isDirectory);
                    for (File fTypeIn : fTypeDirs) {
                        File fTypeOut = new File(featureDirsOut, fTypeIn.getName());
                        this.dirCopy(fTypeIn, fTypeOut, FEATURE_FILES);
                    }
                }
            }
            this.gCopyCount++;
        }
    }

    /**
     * Copy the specified files from the input directory to the output directory.
     *
     * @param dirIn		input directory
     * @param dirOut	output directory (usually must be created)
     * @param files		names of files to copy
     *
     * @throws IOException
     */
    private void dirCopy(File dirIn, File dirOut, String[] files) throws IOException {
        for (String fileName : files) {
            File oldFile = new File(dirIn, fileName);
            if (oldFile.exists()) {
                // This automatically creates the output directory if necessary.
                FileUtils.copyFileToDirectory(new File(dirIn, fileName), dirOut);
                this.fCopyCount++;
            }
        }
    }

    /**
     * Copy all the subsystems.
     *
     * @throws IOException
     */
    private void copySubsystems() throws IOException {
        // Get all the subsystem directories.
        File[] subsystemDirs = this.subsysIn.listFiles(new SubsystemFilter());
        log.info("{} subsystem directories found in {}.", subsystemDirs.length, this.subsysIn);
        for (File subsystemIn : subsystemDirs) {
            // Extract the subsystem name and form the output directory.
            String subsystem = subsystemIn.getName();
            String subsystem1 = (this.winFlag ? fixSubsystemName(subsystem) : subsystem);
            File subsystemOut = new File(this.subsysOut, subsystem1);
            if (subsystemOut.exists())
                log.info("Subsystem {} already exists:  skipped.");
            else {
                log.debug("Copying subsystem {}.", subsystem);
                this.dirCopy(subsystemIn, subsystemOut, SUBSYS_FILES);
            }
            sCopyCount++;
        }
    }

    /**
     * Replace invalid characters in the subsystem name.
     *
     * @param subsystem		subsystem name
     *
     * @return the subsystem name in a legal format
     */
    protected String fixSubsystemName(String subsystem) {
        String retVal = subsystem;
        if (StringUtils.containsAny(subsystem, BAD_CHARS)) {
            for (int i = 0; i < BAD_CHARS.length(); i++)
                retVal = retVal.replace(BAD_CHARS.substring(i, i+1), CHAR_MAP[i]);
        }
        return retVal;
    }

}
