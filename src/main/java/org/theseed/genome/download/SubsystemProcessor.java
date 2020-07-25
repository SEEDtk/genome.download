/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Genome;
import org.theseed.genome.GenomeDirectory;
import org.theseed.genome.SubsystemRow;
import org.theseed.io.LineReader;
import org.theseed.io.MarkerFile;
import org.theseed.utils.BaseProcessor;

/**
 * This command processes a CoreSEED subsystem directory and applies the subsystems to the CoreSEED genomes
 * found in the input directory.  The genomes will be updated in place.
 *
 * Subsystems in CoreSEED are stored externally to the genome directory, so all the genomes have to be updated
 * in parallel, which means they are all loaded into memory.  The key aspects of each subsystem are the classifications
 * (found in the "CLASSIFICATION" file), and the spreadsheet.  The spreadsheet contains a list of roles, a marker (//),
 * a list of role types (that is ignored), another marker, and then the spreadsheet itself.  The spreadsheet contains
 * a genome ID in the first column, a variant code in the second, and then peg numbers for the pegs assigned to each
 * role.  If the feature is not a peg, the feature-type will be included.  So "fig|83333.1.peg.4" is "4", but
 * "fig|83333.1.rna.6" is "rna.6".
 *
 * A subsystem is ignored if it does not have the EXCHANGEABLE marker file or if the word "experimental" appears in the
 * classifications.
 *
 * A subsystem row is ignored if the variant code is "-1" or "*-1".
 *
 * The positional parameters are the name of the GTO input directory and the name of the subsystem directory.
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more detailed progress messages
 * -b	batch size for loading genomes (default 100)
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
    /** subsystem classifications */
    private Map<File, String[]> classMap;

    // COMMAND-LINE OPTIONS

    /** genome batch size */
    @Option(name = "-b", aliases = { "--batchSize", "--batch" }, metaVar = "50", usage = "number of genomes to process in each batch")
    private int batchSize;

    /** genome directory */
    @Argument(index = 0, metaVar = "gtoDir", usage = "input genome directory", required = true)
    private File gtoDir;

    /** subsystem directory */
    @Argument(index = 1, metaVar = "FIG/Data/Subsystems", usage = "CoreSEED subsystem directory", required = true)
    private File subsysDir;

    @Override
    protected void setDefaults() {
        this.batchSize = 100;
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
        // We loop through the subsystems, memorizing the classifications.
        File[] subDirs = this.subsysDir.listFiles(new SubsystemFilter());
        this.classMap = new HashMap<File, String[]>(subDirs.length);
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
                    // Now we have the classification and we know this is a subsystem we want to process.  The last
                    // step is to read the spreadsheet file.  From this we get the role list and the feature bindings.
                    log.info("Saving subsystem {}: {}; {}; {}.", subName, classParts[0], classParts[1], classParts[2]);
                    this.classMap.put(subDir, classParts);
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
            // Clear the existing subsystems and store the new genome.
            genome.deleteSubsystems();
            String genomeId = genome.getId();
            this.genomeMap.put(genomeId, genome);
            this.fileMap.put(genomeId, genomes.currFile());
        }
        // Process the residual batch.
        this.processBatch();
        log.info("All done.");
    }

    private void processBatch() throws IOException {
        log.info("Processing batch of {} genomes.", this.genomeMap.size());
        // Loop through the subsystems, finding the role bindings.
        for (Map.Entry<File, String[]> subEntry : this.classMap.entrySet()) {
            File subDir = subEntry.getKey();
            String[] classes = subEntry.getValue();
            String subName = this.dirToName(subDir);
            log.info("Reading spreadsheet for subsystem {}.", subName);
            try (LineReader spreadsheetStream = new LineReader(new File(subDir, "spreadsheet"))) {
                // Get the role names.
                String[] roles = this.readRoles(spreadsheetStream);
                // Skip past the group definitions.
                this.skipGroups(spreadsheetStream);
                // Now we read the actual spreadsheet.
                for (String line : spreadsheetStream) {
                    String[] fields = StringUtils.splitPreserveAllTokens(line, '\t');
                    if(SubsystemRow.isActive(fields[1])) {
                        // This is an active variant, so check the genome.
                        String genomeId = fields[0];
                        Genome genome = this.genomeMap.get(genomeId);
                        if (genome != null) {
                            // Now we have an active row for a genome of interest.
                            this.createSubsystem(genome, subName, classes, roles, fields);
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
     * @param subName	name of the subsystem
     * @param classes	array of the three classification strings
     * @param roles		array of roles in the subsystem
     * @param fields	array of fields in the spreadsheet row
     */
    private void createSubsystem(Genome genome, String subName, String[] classes, String[] roles, String[] fields) {
        // Create the subsystem itself.  Note that field 1 is the variant code.
        SubsystemRow subRow = new SubsystemRow(genome, subName);
        subRow.setVariantCode(fields[1]);
        // Store the classifications.
        subRow.setClassifications(classes[0], classes[1], classes[2]);
        // This will be the feature ID prefix for all features.  Some will also need "peg".
        String prefix = "fig|" + genome.getId() + ".";
        // Now process the roles.
        for (int i = 0; i < roles.length; i++) {
            // Get the pegs for this role.
            subRow.addRole(roles[i]);
            if (i + 2 < fields.length) {
                String[] pegs = StringUtils.split(fields[i + 2], ',');
                for (String peg : pegs) {
                    String fid = (peg.contains(".") ? prefix + peg : prefix + "peg." + peg);
                    subRow.addFeature(roles[i], fid);
                }
            }
        }
    }

    /**
     * Skip the group definitions in a spreadsheet file.
     *
     * @param spreadsheetStream		open spreadsheet file stream
     */
    private void skipGroups(LineReader spreadsheetStream) {
        String line = spreadsheetStream.next();
        while (! line.contentEquals("//")) {
            line = spreadsheetStream.next();
        }
    }

    /**
     * @return an array of role names for this subsystem
     *
     * @param spreadsheetStream		open spreadsheet file stream
     */
    private String[] readRoles(LineReader spreadsheetStream) {
        List<String> roleList = new ArrayList<String>(40);
        String line = spreadsheetStream.next();
        while (! line.contentEquals("//")) {
            roleList.add(StringUtils.substringAfter(line, "\t"));
            line = spreadsheetStream.next();
        }
        String[] retVal = new String[roleList.size()];
        return roleList.toArray(retVal);
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
