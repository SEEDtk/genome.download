/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.theseed.basic.BaseProcessor;
import org.theseed.genome.core.OrganismDirectories;
import org.theseed.io.FileTarget;
import org.theseed.subsystems.SubsystemFilter;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.kohsuke.args4j.Argument;
import org.apache.commons.lang3.StringUtils;

/**
 * This command copies and compresses the small subset of SEED files needed for testing the CoreSEED utilities.
 * This includes parts of the Organisms and Subsystems directories.
 *
 * The positional parameter is the name of the input CoreSEED data directory.  The zip file is produced on the
 * standard output.
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	show more detailed progress messages
 * -o	output file or folder (default uses current date)
 * -t	file target type (default ZIPSTREAM)
 *
 * --win		change directory names that are invalid in Windows
 * --clear		if specified, the target directory (DIR mode only) will be erased before processing
 *
 * @author Bruce Parrello
 *
 */
public class SeedProcessor extends BaseProcessor implements FileTarget.IParms {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(SeedProcessor.class);
    /** input subsystem directory */
    private File subsysIn;
    /** input organism directory */
    private File orgIn;
    /** output subsystem directory */
    private String subsysOut;
    /** output organism directory */
    private String orgOut;
    /** number of genomes copied */
    private int gCopyCount;
    /** number of subsystems copied */
    private int sCopyCount;
    /** set of subsystem IDs processed */
    private Set<String> subIDs;
    /** time stamp of last progress message */
    private long lastMessage;
    /** output file target */
    private FileTarget outputFolder;
    /** list of subsystem file names */
    private static final String[] SUBSYS_FILES = new String[] { "CLASSIFICATION", "EXCHANGABLE", "spreadsheet"
            , "ERRORCOUNT", "curation.log", "checkvariant_definitions", "checkvariant_rules", "VERSION", "notes" };
    /** list of genome file names */
    private static final String[] GENOME_FILES = new String[] {"contigs", "annotations", "GENOME", "TAXONOMY_ID", "TAXONOMY",
            "assigned_functions", "pattyfams.txt", "COMPLETE" };
    /** list of feature directory files */
    private static final String[] FEATURE_FILES = new String[] { "deleted.features", "fasta", "tbl" };
    /** list of invalid characters */
    private static final String BAD_CHARS = "%?\\<>:*";
    /** map of character replacements */
    private static final String[] CHAR_MAP = buildCharMap(BAD_CHARS);

    // COMMAND-LINE OPTIONS

    /** TRUE to translate directory names that are invalid in Windows */
    @Option(name = "--win", usage = "change invalid subsystem names")
    private boolean winFlag;

    /** output file or directory */
    @Option(name = "-o", aliases = { "--output", "--out" }, metaVar = "CoreCopy.zip", usage = "output zip file")
    private File outFile;

    /** file target type */
    @Option(name = "-t", aliases = { "--type" }, usage = "type of output (file system, archive, etc)")
    private FileTarget.Type outType;

    /** if specified, the target DIR-mode directory will be erased before processing */
    @Option(name = "--clear", usage = "erase output directory before processing (DIR mode only)")
    private boolean clearFlag;

    /** input SEED data directory */
    @Argument(index = 0, metaVar = "/vol/core-seed/FIGdisk/FIG/Data", usage = "input coreSEED directory", required = true)
    private File coreDir;

    @Override
    protected void setDefaults() {
        this.sCopyCount = 0;
        this.gCopyCount = 0;
        this.outFile = null;
        this.clearFlag = false;
        this.outType = FileTarget.Type.ZIPSTREAM;
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
    protected void validateParms() throws IOException {
        // Insure the input directory is valid.
        if (! this.coreDir.isDirectory())
            throw new FileNotFoundException("Input coreSEED directory " + this.coreDir + " not found or invalid.");
        this.subsysIn = new File(this.coreDir, "Subsystems");
        this.orgIn = new File(this.coreDir, "Organisms");
        if (! this.subsysIn.isDirectory() || ! this.orgIn.isDirectory())
            throw new FileNotFoundException("Input coreSEED directory " + this.coreDir + " is missing Organisms or Subsystems sub-directory.");
        // The output directory strings are used to generate directory names for the zip file.
        this.subsysOut = "Subsystems/";
        this.orgOut = "Organisms/";
        // Create the subsystem ID map if we are doing the Windows filtering.
        if (this.winFlag)
            this.subIDs = new HashSet<>(2000);
        // Set up the output file stream.
        this.outputFolder = this.outType.create(this, this.outFile);
        log.info("Output will be to {} {}.", this.outType, this.outputFolder.getOutName());
    }

    @Override
    protected void runCommand() throws Exception {
        // Initialize the output zip stream.
        try {
            // Set up the zip stream.
            // We must add the master directories.
            this.outputFolder.createDirectory(this.subsysOut);
            this.outputFolder.createDirectory(this.orgOut);
            // Copy the subsystems.
            this.copySubsystems();
            // Copy the genomes.
            this.copyGenomes();
            // Denote we're done.
            log.info("All done.  {} genomes, {} subsystems, and {} total files copied.", this.gCopyCount, this.sCopyCount,
                    this.outputFolder.getCopyCount());
        } finally {
            if (this.outputFolder != null)
                this.outputFolder.close();
        }
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
        this.lastMessage = System.currentTimeMillis();
        for (String genomeId : orgDirs) {
            File genomeIn = new File(this.orgIn, genomeId);
            // Insure the genome is real.
            File deleteFile = new File(genomeIn, "DELETED");
            if (deleteFile.exists()) {
                log.debug("Skipping deleted genome {}.", genomeId);
            } else {
                String genomeOut = this.orgOut + genomeId + "/";
                // Copy the basic genome files.
                log.debug("Copying genome {}.", genomeId);
                this.outputFolder.dirCopy(genomeIn, genomeOut, GENOME_FILES);
                // Now we need to process each feature directory.
                File featureDirsIn = new File(genomeIn, "Features");
                String featureDirsOut = genomeOut + "Features/";
                if (featureDirsIn.isDirectory()) {
                    File[] fTypeDirs = featureDirsIn.listFiles(File::isDirectory);
                    for (File fTypeIn : fTypeDirs) {
                        String fTypeOut = featureDirsOut + fTypeIn.getName() + "/";
                        this.outputFolder.dirCopy(fTypeIn, fTypeOut, FEATURE_FILES);
                    }
                }
                this.gCopyCount++;
                this.checkLog(gCopyCount, "genomes");
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
        this.lastMessage = System.currentTimeMillis();
        for (File subsystemIn : subsystemDirs) {
            // Extract the subsystem name and form the output directory.
            String subsystem = subsystemIn.getName();
            String subsystem1 = subsystem;
            if (this.winFlag) {
                // For windows, we have to handle directory name pathologies.
                subsystem1 = this.fixSubsystemName(subsystem);
                String checkName = subsystem1.toLowerCase();
                while (this.subIDs.contains(checkName)) {
                    // Here the new subsystem differs from an old one only by capitalization.
                    subsystem1 += "_";
                    checkName += "_";
                }
                // Now we have a unique version of the subsystem ID that works on Windows.
                this.subIDs.add(checkName);
                if (log.isWarnEnabled() && ! subsystem1.contentEquals(subsystem))
                    log.warn("Pathological subsystem ID \"{}\" converted to \"{}\".", subsystem, subsystem1);
            }
            String subsystemOut = this.subsysOut + subsystem1 + "/";
            log.debug("Copying subsystem {}.", subsystem);
            this.outputFolder.dirCopy(subsystemIn, subsystemOut, SUBSYS_FILES);
            sCopyCount++;
            this.checkLog(sCopyCount, "subsystems");
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

    /**
     * Write a log message showing progress if we have not had one recently.
     *
     * @param count		number of objects copied
     * @param type		type of objects copied (as a plural)
     */
    private void checkLog(int count, String type) {
        long now = System.currentTimeMillis();
        if (log.isInfoEnabled() && now - this.lastMessage >= 5000) {
            this.lastMessage = now;
            log.info("{} {} copied.", count, type);
        }
    }

    @Override
    public boolean shouldErase() {
        return this.clearFlag;
    }

}
