/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.theseed.genome.core.OrganismDirectories;
import org.theseed.subsystems.SubsystemFilter;
import org.theseed.utils.BaseProcessor;
import org.kohsuke.args4j.Option;
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
 * -o	optional output file, if the standard output is not used
 *
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
    private String subsysOut;
    /** output organism directory */
    private String orgOut;
    /** number of genomes copied */
    private int gCopyCount;
    /** number of subsystems copied */
    private int sCopyCount;
    /** number of files copied */
    private int fCopyCount;
    /** set of subsystem IDs processed */
    private Set<String> subIDs;
    /** file stream for output, if any */
    private FileOutputStream fileStream;
    /** zip file output stream */
    private ZipOutputStream zipStream;
    /** read buffer */
    private byte[] buffer;
    /** list of subsystem file names */
    private static final String[] SUBSYS_FILES = new String[] { "CLASSIFICATION", "EXCHANGABLE", "spreadsheet" };
    /** list of genome file names */
    private static final String[] GENOME_FILES = new String[] {"contigs", "annotations", "GENOME", "TAXONOMY_ID", "TAXONOMY",
            "assigned_functions", "pattyfams.txt" };
    /** list of feature directory files */
    private static final String[] FEATURE_FILES = new String[] { "deleted.features", "fasta", "tbl" };
    /** list of invalid characters */
    private static final String BAD_CHARS = "?\\<>:*";
    /** map of character replacements */
    private static final String[] CHAR_MAP = buildCharMap(BAD_CHARS);
    /** proposed buffer size */
    private static final int BUFFER_SIZE = 4096;

    // COMMAND-LINE OPTIONS

    /** TRUE to translate directory names that are invalid in Windows */
    @Option(name = "--win", usage = "change invalid subsystem names")
    private boolean winFlag;

    /** output zip file */
    @Option(name = "-o", aliases = { "--ouput", "--out" }, metaVar = "CoreCopy.zip", usage = "output zip file")
    private File outZipFile;

    /** input SEED data directory */
    @Argument(index = 0, metaVar = "/vol/core-seed/FIGdisk/FIG/Data", usage = "input coreSEED directory", required = true)
    private File coreDir;

    @Override
    protected void setDefaults() {
        this.sCopyCount = 0;
        this.gCopyCount = 0;
        this.fCopyCount = 0;
        this.outZipFile = null;
        this.fileStream = null;
        this.zipStream = null;
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
        // The output directory strings are used to generate directory names for the zip file.
        this.subsysOut = "Subsystems/";
        this.orgOut = "Organisms/";
        // Allocate the read buffer.
        this.buffer = new byte[BUFFER_SIZE];
        // Create the subsystem ID map if we are doing the Windows filtering.
        if (this.winFlag)
            this.subIDs = new HashSet<String>(2000);
        // Set up the output file stream.
        if (this.outZipFile != null) {
            log.info("Output will be to {}.", this.outZipFile);
            this.fileStream = new FileOutputStream(this.outZipFile);
        }
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // Initialize the output zip stream.
        try {
            // Set up the zip stream.
            OutputStream outStream = (this.fileStream == null ? System.out : this.fileStream);
            this.zipStream = new ZipOutputStream(outStream);
            // We must add the master directories.
            this.createDirectory(this.subsysOut);
            this.createDirectory(this.orgOut);
            // Copy the subsystems.
            this.copySubsystems();
            // Copy the genomes.
            this.copyGenomes();
            // Denote we're done.
            outStream.flush();
            log.info("All done.  {} genomes, {} subsystems, and {} total files copied.", this.gCopyCount, this.sCopyCount,
                    this.fCopyCount);
        } finally {
            if (this.zipStream != null)
                this.zipStream.close();
            if (this.fileStream != null)
                this.fileStream.close();
        }
    }

    /**
     * Create a directory entry in the output zip file.
     *
     * @param newDir	name of the new directory
     *
     * @throws IOException
     */
    private void createDirectory(String newDir) throws IOException {
        this.zipStream.putNextEntry(new ZipEntry(newDir));
        this.zipStream.closeEntry();

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
            String genomeOut = this.orgOut + genomeId + "/";
            // Copy the basic genome files.
            log.debug("Copying genome {}.", genomeId);
            this.dirCopy(genomeIn, genomeOut, GENOME_FILES);
            // Now we need to process each feature directory.
            File featureDirsIn = new File(genomeIn, "Features");
            File featureDirsOut = new File(genomeOut, "Features");
            if (featureDirsIn.isDirectory()) {
                File[] fTypeDirs = featureDirsIn.listFiles(File::isDirectory);
                for (File fTypeIn : fTypeDirs) {
                    String fTypeOut = featureDirsOut + fTypeIn.getName() + "/";
                    this.dirCopy(fTypeIn, fTypeOut, FEATURE_FILES);
                }
            }
            this.gCopyCount++;
        }
    }

    /**
     * Copy the specified files from the input directory to the zip file.  The copy
     * assumes the directory does not exist in the ZIP file yet.  It must not be
     * used again.
     *
     * @param dirIn		input directory
     * @param dirOut	output directory name
     * @param files		names of files to copy
     *
     * @throws IOException
     */
    private void dirCopy(File dirIn, String dirOut, String[] files) throws IOException {
        this.createDirectory(dirOut);
        for (String fileName : files) {
            File oldFile = new File(dirIn, fileName);
            if (oldFile.exists()) {
                // This automatically creates the output directory.
                ZipEntry fileEntry = new ZipEntry(dirOut + fileName);
                this.zipStream.putNextEntry(fileEntry);
                try (FileInputStream oldStream = new FileInputStream(oldFile)) {
                    for (int len = oldStream.read(buffer); len >= 0; len = oldStream.read(buffer))
                        this.zipStream.write(this.buffer, 0, len);
                }
                this.zipStream.closeEntry();
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
            this.dirCopy(subsystemIn, subsystemOut, SUBSYS_FILES);
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
