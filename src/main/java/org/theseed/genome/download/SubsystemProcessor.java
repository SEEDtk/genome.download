/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.genome.Feature;
import org.theseed.genome.Genome;
import org.theseed.genome.iterator.GenomeSource;
import org.theseed.io.LineReader;
import org.theseed.io.MarkerFile;
import org.theseed.proteins.RoleSet;
import org.theseed.subsystems.CountingSpreadsheetAnalyzer;
import org.theseed.subsystems.ProjectionSpreadsheetAnalyzer;
import org.theseed.subsystems.SpreadsheetAnalyzer;
import org.theseed.subsystems.SubsystemProjector;
import org.theseed.subsystems.SubsystemSpec;
import org.theseed.subsystems.TrackingSpreadsheetAnalyzer;
import org.theseed.subsystems.VariantId;
import org.theseed.subsystems.core.CoreSubsystem;

/**
 * This command processes a CoreSEED data directory and computes the subsystem projector.
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
 * A subsystem row is ignored if the variant code is "-1", "inactive", or "*-1".
 *
 * The positional parameters are the name of the CoreSEED data directory and the name of the data directory for output files.
 * The projector will be called "variants.tbl", the error file "errors.tbl", and the role-counts file "roleCounts.tbl".
 * In addition, a role definition file will be produced in "roles.in.subsystems", and a list of complete genomes in
 * "complete.tbl".
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more detailed progress messages
 * -b	batch size for loading genomes (default 100)
 *
 * --clear		erase the output directory before processing
 * --roles		an existing roles.in.subsystems file to pre-load
 * --filter		if specified, a file of subsystem names; only the named subsystems will be checked
 * --save		if specified, the name of a directory in which to store GTOs for the CoreSEED
 *
 *  @author Bruce Parrello
 *
 */
public class SubsystemProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SubsystemProcessor.class);
    /** genome hash */
    private Map<String, Genome> genomeMap;
    /** genome directory */
    private GenomeSource genomes;
    /** subsystem information to be saved for projection */
    private SubsystemProjector projector;
    /** map of subsystem directories to subsystem specifiers */
    private Map<File, SubsystemSpec> subDirMap;
    /** list of analyzers to run */
    private List<SpreadsheetAnalyzer> analyzers;
    /** bad-feature output file */
    private File errorFile;
    /** output file for role counts */
    private File roleCountFile;
    /** output file for projector */
    private File projectorFile;
    /** output file for complete-genome list */
    private File completeFile;
    /** output file for blank annotations */
    private File blankFile;
    /** subsystem directory */
    private File subsysDir;

    // COMMAND-LINE OPTIONS

    /** genome batch size */
    @Option(name = "-b", aliases = { "--batchSize", "--batch" }, metaVar = "50", usage = "number of genomes to process in each batch")
    private int batchSize;

    /** if specified, the output directory will be cleared before processing */
    @Option(name = "--clear", usage = "if specified, the output directory will be erased before processing")
    private boolean clearFlag;

    /** if specified, a role definition file to pre-load (new roles found will be added) */
    @Option(name = "--roles", usage = "option role definition file to pre-load")
    private File roleFile;

    /** if specified, the name of a file containing subsystem names in the first column; only the named subsystems will be checked */
    @Option(name = "--filter", metaVar = "ssNames.tbl", usage = "file of subsystem names to check (tab-separated with headers)")
    private File filterFile;

    /** if specified, the name of a directory to contain GTOs for the CoreSEED genomes */
    @Option(name = "--save", metaVar = "coreGtoDir", usage = "optional output directory for CoreSEED GTOs; always erased")
    private File coreGtoDir;

    /** CoreSEED input directory */
    @Argument(index = 0, metaVar = "FIGdisk/FIG/Data", usage = "main CoreSEED directory")
    private File coreDir;

    /** output directory */
    @Argument(index = 1, metaVar = "outDir", usage = "data file output directory")
    private File outDir;

    @Override
    protected void setDefaults() {
        this.batchSize = 100;
        this.analyzers = new ArrayList<SpreadsheetAnalyzer>(10);
        this.clearFlag = false;
        this.roleFile = null;
        this.filterFile = null;
        this.coreGtoDir = null;
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        if (this.batchSize < 1)
            throw new ParseFailureException("Invalid batch size.  Must be 1 or greater.");
        if (! this.coreDir.isDirectory())
            throw new FileNotFoundException("CoreSEED data directory " + this.coreDir + " not found or invalid.");
        this.subsysDir = new File(this.coreDir, "Subsystems");
        if (! this.subsysDir.isDirectory())
            throw new FileNotFoundException("Subsystem directory " + this.subsysDir + " not found or invalid.");
        // Here we get the input directory and create the genome map for storing batches.
        log.info("Scanning genome directory for {}.", this.coreDir);
        this.genomes = GenomeSource.Type.CORE.create(this.coreDir);
        log.info("{} genomes found to process.  Batch size is {}.", this.genomes.size(), this.batchSize);
        this.genomeMap = new HashMap<String, Genome>(this.batchSize);
        // Create the subsystem projector and the directory map.
        this.projector = new SubsystemProjector(this.roleFile);
        this.subDirMap = new HashMap<File, SubsystemSpec>();
        // Set up the output directory.
        if (! this.outDir.isDirectory()) {
            log.info("Creating output directory {}.", this.outDir);
            FileUtils.forceMkdir(this.outDir);
        } else if (this.clearFlag) {
            log.info("Erasing output directory {}.", this.outDir);
            FileUtils.cleanDirectory(this.outDir);
        } else
            log.info("Output files will be stored in directory {}.", this.outDir);
        // Set up the optional output directory for CoreSEED GTOs.
        if (this.coreGtoDir != null) {
            // Here we need to set up the output directory for the CoreSEED GTOs.
            if (! this.coreGtoDir.isDirectory()) {
                log.info("Creating GTO output directory {}.", this.coreGtoDir);
                FileUtils.forceMkdir(this.coreGtoDir);
            } else {
                log.info("Erasing GTO output directory {}.", this.coreGtoDir);
                FileUtils.cleanDirectory(this.coreGtoDir);
            }
        }
        // Compute the output files.
        this.projectorFile = new File(this.outDir, "variants.tbl");
        this.errorFile = new File(this.outDir, "errors.tbl");
        this.roleCountFile = new File(this.outDir, "roleCounts.tbl");
        this.completeFile = new File(this.outDir, "complete.tbl");
        this.blankFile = new File(this.outDir, "blank.tbl");
        // Create the analyzers.
        this.analyzers.add(new ProjectionSpreadsheetAnalyzer(this.projector, this.projectorFile));
        this.analyzers.add(new TrackingSpreadsheetAnalyzer(this.projector, this.errorFile));
        this.analyzers.add(new CountingSpreadsheetAnalyzer(this.projector, this.roleCountFile));
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // We loop through the subsystems, memorizing the classifications.
        List<File> subDirs = CoreSubsystem.getFilteredSubsystemDirectories(this.coreDir, this.filterFile);
        log.info("{} subsystem directories found with spreadsheets.", subDirs.size());
        for (File subDir : subDirs) {
            // Compute the subsystem name from the directory.
            String subName = CoreSubsystem.dirToName(subDir);
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
        // Now we load the genomes in batches and process them.  We also generate the complete-genome
        // list and blank-annotation list here.
        try (PrintWriter completeWriter = new PrintWriter(this.completeFile);
                PrintWriter blankWriter = new PrintWriter(this.blankFile)) {
            completeWriter.println("genome_id\tgenome_name");
            blankWriter.println("genome_id\tfeature_id\tgenome_name");
            for (Genome genome : this.genomes) {
                // Insure there is room for another genome.
                if (this.genomeMap.size() >= this.batchSize) {
                    this.processBatch();
                    this.genomeMap.clear();
                }
                // Clear the existing subsystems and store the new genome in the maps.  The subsystems are
                // populated when we process the batch.
                genome.clearSubsystems();
                // Do the completeness check.
                String genomeId = genome.getId();
                String genomeName = genome.getName();
                this.genomeMap.put(genomeId, genome);
                if (genome.isComplete())
                    completeWriter.println(genomeId + "\t" + genomeName);
                // Scan for features with missing annotations.
                for (Feature feat : genome.getPegs()) {
                    if (StringUtils.isBlank(feat.getFunction()))
                        blankWriter.println(genomeId + "\t" + feat.getId() + "\t" + genomeName);
                }
            }
            // Process the residual batch.
            this.processBatch();
        }
        // Write out an extra copy of the role map
        log.info("Writing roles.in.subsystems.");
        File roleFile = new File(this.outDir, "roles.in.subsystems");
        this.projector.usefulRoles().save(roleFile);
        // Write out a rule report.
        File ruleFile = new File(this.outDir, "rules.txt");
        this.projector.ruleReport(ruleFile);
        // Terminate the analyzers.
        for (SpreadsheetAnalyzer analyzer : this.analyzers)
            analyzer.close();
    }

    private void processBatch() throws IOException {
        log.info("Processing batch of {} genomes.", this.genomeMap.size());
        // Create role maps for all the genomes.
        Map<String, Map<String, Set<String>>> gRoleMaps = new HashMap<>(this.batchSize);
        for (Map.Entry<String, Genome> genomeEntry : this.genomeMap.entrySet()) {
            final Genome genome = genomeEntry.getValue();
            log.info("Computing role map for {}.", genome);
            Map<String, Set<String>> roleMap = this.projector.computeRoleMap(genome);
            gRoleMaps.put(genomeEntry.getKey(), roleMap);
        }
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
                            this.createSubsystem(genome, gRoleMaps.get(genomeId), subsystem, fields);
                        }
                    }
                }
            }
        }
        // Check to see if we need to write out the genomes.
        if (this.coreGtoDir != null) {
            log.info("Preparing to save {} genomes to {}.", this.genomeMap.size(), this.coreGtoDir);
            for (Genome genome : this.genomeMap.values()) {
                File outFile = new File(this.coreGtoDir, genome.getId() + ".gto");
                log.info("Saving genome to {}.", outFile);
                genome.save(outFile);
            }
        }
    }

    /**
     * Create the subsystem row in the specified genome for the current subsystem.
     *
     * @param genome	genome implementing the subsystem
     * @param roleMap 	role map for the genome
     * @param subsystem	subsystem specification
     * @param fields	array of fields in the spreadsheet row
     */
    private void createSubsystem(Genome genome, Map<String, Set<String>> roleMap, SubsystemSpec subsystem, String[] fields) {
        // Initialize for processing this row.  Note that field 1 is the variant code.
        for (SpreadsheetAnalyzer analyzer : this.analyzers)
            analyzer.openRow(genome, roleMap, subsystem, fields[1]);
        // This will be the feature ID prefix for all features.  Some will also need "peg".
        String prefix = "fig|" + genome.getId() + ".";
        // Now process the roles.
        List<String> roles = subsystem.getRoles();
        for (int i = 0; i < roles.size(); i++) {
            // Get the roleset for this role.
            String function = roles.get(i);
            RoleSet cRoles = projector.getRoleIds(function);
            // Get the pegs for this role.
            if (i + 2 < fields.length) {
                String[] pegs = StringUtils.split(fields[i + 2], ',');
                for (String peg : pegs) {
                    String fid = (peg.contains(".") ? prefix + peg : prefix + "peg." + peg);
                    // Look for the feature.  We need to make sure it exists and implements the
                    // cell's roles.
                    Feature feat = genome.getFeature(fid);
                    RoleSet fRoles = projector.getRoleIds(feat);
                    if (fRoles.contains(cRoles)) {
                        // Here we are good.  Add this cell to the variant.
                        for (SpreadsheetAnalyzer analyzer : this.analyzers)
                            analyzer.goodCell(i, feat);
                    } else {
                        // Here the subsystem refers to a feature that does not exist or has the wrong role.
                        // See if there is a substitute.
                        Set<String> fids = cRoles.featureSet(roleMap);
                        if (fids != null) {
                            // Features exist that contain the proper role.
                            for (SpreadsheetAnalyzer analyzer : this.analyzers)
                                analyzer.badCell(i, fid, function, fids);
                        } else {
                            // No feature exists that contains the proper role.
                            for (SpreadsheetAnalyzer analyzer : this.analyzers) {
                                if (feat == null)
                                    analyzer.badCell(i, fid, function);
                                else
                                    analyzer.badCell(i, feat, function);
                            }
                        }
                    }
                }
            }
        }
        // Finish processing for this role.
        for (SpreadsheetAnalyzer analyzer : this.analyzers)
            analyzer.closeRow();
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


}
