/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Feature;
import org.theseed.genome.Genome;
import org.theseed.genome.GenomeDirectory;
import org.theseed.genome.SubsystemRow;
import org.theseed.io.LineReader;
import org.theseed.io.MarkerFile;
import org.theseed.subsystems.SubsystemProjector;
import org.theseed.subsystems.SubsystemSpec;
import org.theseed.subsystems.VariantSpec;
import org.theseed.subsystems.VariantId;
import org.theseed.utils.BaseProcessor;

/**
 * This command processes a CoreSEED subsystem directory and applies the subsystems to the CoreSEED genomes
 * found in the input directory.  The genomes will be updated in place.  It also optionally produces a data file
 * that can be used to project subsystems onto GTOs.
 *
 * Subsystems in CoreSEED are stored externally to the genome directory, so all the genomes have to be updated
 * in parallel, which means they are all loaded into memory.  The key aspects of each subsystem are the classifications
 * (found in the "CLASSIFICATION" file), and the spreadsheet.  The spreadsheet contains a list of roles, a marker (//),
 * a list of role types (that is ignored), another marker, and then the spreadsheet itself.  The spreadsheet contains
 * a genome ID in the first column, a variant code in the second, and then peg numbers for the pegs assigned to each
 * role.  If the feature is not a peg, the feature-type will be included.  So "fig|83333.1.peg.4" is "4", but
 * "fig|83333.1.rna.6" is "rna.6".
 *
 * A subsystem is ignored if it does not have the EXCHANGABLE marker file or if the word "experimental" appears in the
 * classifications.
 *
 * A subsystem row is ignored if the variant code is "-1" or "*-1".
 *
 * The positional parameters are the name of the GTO input directory, the name of the subsystem directory, and
 * the name of the data directory for output files.
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more detailed progress messages
 * -b	batch size for loading genomes (default 100)
 * -o	output file for subsystem definitions
 *
 * @author Bruce Parrello
 *
 */
public class SubsystemProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SubsystemProcessor.class);
    /** genome hash */
    private Map<String, Genome> genomeMap;
    /** genome file hash */
    private Map<String, File> fileMap;
    /** genome directory */
    private GenomeDirectory genomes;
    /** subsystem information to be saved for projection */
    private SubsystemProjector projector;
    /** map of subsystem directories to subsystem specifiers */
    private Map<File, SubsystemSpec> subDirMap;
    /** number of duplicate variant specifications found */
    private int dupCount;
    /** number of missing features found */
    private int badFeatureCount;
    /** number of variant instances skipped */
    private int skipCount;
    /** number of features without protein families */
    private int roleCount;

    // COMMAND-LINE OPTIONS

    /** genome batch size */
    @Option(name = "-b", aliases = { "--batchSize", "--batch" }, metaVar = "50", usage = "number of genomes to process in each batch")
    private int batchSize;

    /** subsystem projector output file */
    @Option(name = "-o", aliases = { "--projector" }, metaVar = "variants.tbl", usage = "output file for subsystem projection data")
    private File projectorFile;

    /** genome directory */
    @Argument(index = 0, metaVar = "gtoDir", usage = "input genome directory", required = true)
    private File gtoDir;

    /** subsystem directory */
    @Argument(index = 1, metaVar = "FIG/Data/Subsystems", usage = "CoreSEED subsystem directory", required = true)
    private File subsysDir;

    @Override
    protected void setDefaults() {
        this.batchSize = 100;
        this.projectorFile = null;
    }

    @Override
    protected boolean validateParms() throws IOException {
        if (this.batchSize < 1)
            throw new IllegalArgumentException("Invalid batch size.  Must be 1 or greater.");
        if (! this.subsysDir.isDirectory())
            throw new FileNotFoundException("Subsystem directory " + this.subsysDir + " not found or invalid.");
        if (! gtoDir.isDirectory())
            throw new FileNotFoundException("GTO directory " + this.gtoDir + " not found or invalid.");
        // Here we get the input directory and create the genome map for storing batches.
        log.info("Scanning input directory {}.", this.gtoDir);
        this.genomes = new GenomeDirectory(gtoDir);
        log.info("{} genomes found to process.  Batch size is {}.", this.genomes.size(), this.batchSize);
        this.genomeMap = new HashMap<String, Genome>(this.batchSize);
        this.fileMap = new HashMap<String, File>(this.batchSize);
        // Create the subsystem projector and the directory map.
        this.projector = new SubsystemProjector();
        this.subDirMap = new HashMap<File, SubsystemSpec>();
        // If we have a projector output file, verify we can write to it.
        if (this.projectorFile != null) {
            FileOutputStream testStream = new FileOutputStream(this.projectorFile);
            testStream.close();
        }
        return true;
    }

    /**
     * This is a file filter that only accepts directories containing subsystem spreadsheets.
     */
    public class SubsystemFilter implements FileFilter {

        @Override
        public boolean accept(File pathname) {
            boolean retVal = pathname.isDirectory();
            if (retVal) {
                File spreadsheetFile = new File(pathname, "spreadsheet");
                retVal = spreadsheetFile.canRead();
            }
            return retVal;
        }

    }

    @Override
    protected void runCommand() throws Exception {
        // Initialize the counts.
        this.badFeatureCount = 0;
        this.dupCount = 0;
        this.skipCount = 0;
        this.roleCount = 0;
        // We loop through the subsystems, memorizing the classifications.
        File[] subDirs = this.subsysDir.listFiles(new SubsystemFilter());
        log.info("{} subsystem directories found with spreadsheets.", subDirs.length);
        for (File subDir : subDirs) {
            // Compute the subsystem name from the directory.
            String subName = dirToName(subDir);
            File exchangeMarker = new File(subDir, "EXCHANGABLE");
            if (! exchangeMarker.exists()) {
                log.info("Skipping private subsystem {}.", subName);
            } else {
                // Read the classifications here.  There are supposed to be three, but it is not guaranteed.
                File classFile = new File(subDir, "CLASSIFICATION");
                String classString = "";
                if (classFile.canRead())
                    classString = MarkerFile.read(classFile);
                if (classString.toLowerCase().contains("experimental")) {
                    log.info("Skipping experimental subsystem {}.", subName);
                } else {
                    String classParts[] = {"", "", ""};
                    int pos = 0;
                    int start = 0;
                    for (int i = 0; i < classString.length(); i++) {
                        if (classString.charAt(i) == '\t') {
                            classParts[pos] = StringUtils.substring(classString, start, i);
                            start = i + 1;
                            pos++;
                        }
                    }
                    if (start < classString.length())
                        classParts[pos] = StringUtils.substring(classString, start);
                    // Now we have the classification and we know this is a subsystem we want to process.  Save its
                    // information.
                    log.info("Saving subsystem {}: {}; {}; {}.", subName, classParts[0], classParts[1], classParts[2]);
                    SubsystemSpec subsystem = new SubsystemSpec(subName);
                    subsystem.setClassifications(classParts);
                    this.subDirMap.put(subDir, subsystem);
                    // The last step is to read the spreadsheet file.  From this we get the role list.  We will read it
                    // again to get the feature bindings for each genome batch.
                    try (LineReader subStream = new LineReader(new File(subDir, "spreadsheet"))) {
                        for (String line = subStream.next(); ! line.contentEquals(SubsystemProjector.END_MARKER);
                                line = subStream.next())
                            subsystem.addRole(StringUtils.substringAfter(line, "\t"));
                    }
                    this.projector.addSubsystem(subsystem);
                }
            }
        }
        // Now we load the genomes in batches and process them.
        for (Genome genome : this.genomes) {
            // Insure there is room for another genome.
            if (this.genomeMap.size() >= this.batchSize) {
                this.processBatch();
                this.genomeMap.clear();
                this.fileMap.clear();
            }
            // Clear the existing subsystems and store the new genome in the maps.  The subsystems are
            // populated when we process the batch.
            genome.clearSubsystems();
            String genomeId = genome.getId();
            this.genomeMap.put(genomeId, genome);
            this.fileMap.put(genomeId, genomes.currFile());
        }
        // Process the residual batch.
        this.processBatch();
        // Output the subsystem projector.
        log.info("Saving subsystem definitions to {}.", this.projectorFile);
        projector.save(this.projectorFile);
        log.info("All done. {} bad features, {} without protein families, {} variant specs ({} duplicates, {} skipped).",
                this.badFeatureCount, this.roleCount, projector.getVariants().size(), this.dupCount, this.skipCount);
    }

    private void processBatch() throws IOException {
        log.info("Processing batch of {} genomes.", this.genomeMap.size());
        // Loop through the subsystems, finding the role bindings.
        for (Map.Entry<File, SubsystemSpec> subEntry : this.subDirMap.entrySet()) {
            File subDir = subEntry.getKey();
            SubsystemSpec subsystem = subEntry.getValue();
            log.info("Reading spreadsheet for subsystem {}.", subsystem.getName());
            try (LineReader spreadsheetStream = new LineReader(new File(subDir, "spreadsheet"))) {
                // Skip past the role names and group definitions.
                this.skipGroups(spreadsheetStream);
                // Now we read the actual spreadsheet.
                for (String line : spreadsheetStream) {
                    String[] fields = StringUtils.splitPreserveAllTokens(line, '\t');
                    if(VariantId.isActive(fields[1])) {
                        // This is an active variant, so check the genome.
                        String genomeId = fields[0];
                        Genome genome = this.genomeMap.get(genomeId);
                        if (genome != null) {
                            // Now we have an active row for a genome of interest.
                            this.createSubsystem(genome, subsystem, fields);
                        }
                    }
                }
            }
        }
        // Finally, we save the genomes.
        for (Map.Entry<String, Genome> genomeEntry : this.genomeMap.entrySet()) {
            String genomeId = genomeEntry.getKey();
            File outFile = this.fileMap.get(genomeId);
            Genome genome = genomeEntry.getValue();
            log.info("Saving {} to {}.", genome, outFile);
            genome.update(outFile);
        }

    }

    /**
     * Create the subsystem row in the specified genome for the current subsystem.
     *
     * @param genome	genome implementing the subsystem
     * @param subsystem	subsystem specification
     * @param fields	array of fields in the spreadsheet row
     */
    private void createSubsystem(Genome genome, SubsystemSpec subsystem, String[] fields) {
        // Create the subsystem itself.  Note that field 1 is the variant code.
        SubsystemRow subRow = new SubsystemRow(genome, subsystem.getName());
        subRow.setVariantCode(fields[1]);
        // Create a variant spec.
        VariantSpec variant = new VariantSpec(subsystem, fields[1]);
        // Denote that so far this variant is good.
        boolean good = true;
        // Store the classifications in the subsystem row.
        subRow.setClassifications(subsystem.getClassifications());
        // This will be the feature ID prefix for all features.  Some will also need "peg".
        String prefix = "fig|" + genome.getId() + ".";
        // Now process the roles.
        List<String> roles = subsystem.getRoles();
        for (int i = 0; i < roles.size(); i++) {
            // Get the pegs for this role.
            String role = roles.get(i);
            subRow.addRole(role);
            if (i + 2 < fields.length) {
                String[] pegs = StringUtils.split(fields[i + 2], ',');
                for (String peg : pegs) {
                    String fid = (peg.contains(".") ? prefix + peg : prefix + "peg." + peg);
                    // Look for the feature.
                    Feature feat = genome.getFeature(fid);
                    if (feat == null) {
                        log.warn("Invalid feature ID {} found in subsystem {}.", fid, subsystem);
                        this.badFeatureCount++;
                        good = false;
                    } else {
                        // Put this feature in the subsystem row.
                        subRow.addFeature(role, fid);
                        // Add this cell to the variant.
                        variant.setCell(i, projector);
                    }
                }
            }
        }
        // Roles processed for this subsystem row.  If we have a valid variant, save it.
        if (good) {
            boolean isNew = this.projector.addVariant(variant);
            if (! isNew)
                this.dupCount++;
            else
                log.debug("Added new {}.", variant);
        } else {
            this.skipCount++;
            log.info("Skipped {} in {}.", variant, genome);
        }
    }

    /**
     * Skip the role and group definitions in a spreadsheet file.
     *
     * @param spreadsheetStream		open spreadsheet file stream
     */
    private void skipGroups(LineReader spreadsheetStream) {
        int markersFound = 0;
        while (markersFound < 2) {
            String line = spreadsheetStream.next();
            if (line.contentEquals(SubsystemProjector.END_MARKER))
                markersFound++;
        }
    }

    /**
     * @return the name of the subsystem in the specified directory
     *
     * @param subDir	subsystem directory of interest
     */
    protected String dirToName(File subDir) {
        return StringUtils.replace(subDir.getName(), "_", " ");
    }


}