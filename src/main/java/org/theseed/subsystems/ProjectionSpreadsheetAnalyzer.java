/**
 *
 */
package org.theseed.subsystems;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;


import org.theseed.genome.Feature;
import org.theseed.genome.Genome;

/**
 * This analyzer projects the subsystems into the genomes and optionally writes the projector to a file.
 *
 * @author Bruce Parrello
 *
 */
public class ProjectionSpreadsheetAnalyzer extends SpreadsheetAnalyzer {

    // FIELDS
    /** output file */
    private File projectorFile;
    /** current variant specification */
    private VariantSpec variant;
    /** TRUE if this variant can be projected */
    private boolean goodVariant;
    /** number of rows processed */
    private int totalCount;
    /** number of rows stored */
    private int storedCount;
    /** number of distinct specifications */
    private int specCount;

    /**
     * Set up to write out a completed subsystem projector.
     *
     * @param projector		subsystem projector being constructed
     * @param outFile		designated output file
     */
    public ProjectionSpreadsheetAnalyzer(SubsystemProjector projector, File outFile) {
        super(projector);
        this.projectorFile = outFile;
        if (outFile != null) {
            log.info("Subsystem projector will be written to {}.", outFile);
            // Verify that we can write to the projector file.
            try {
                FileOutputStream testStream = new FileOutputStream(this.projectorFile);
                testStream.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        // Clear the counters.
        this.totalCount = 0;
        this.storedCount = 0;
        this.specCount = 0;
    }

    @Override
    protected void initializeRow(String variantCode) {
        // Create the variant specification we are building from this row.
        this.variant = new VariantSpec(this.getSubsystem(), variantCode);
        this.goodVariant = true;
    }

    @Override
    protected void recordGoodCell(int idx, Feature feat) {
        activateCell(idx);
    }

    @Override
    protected void recordMissingFeature(int idx, String fid, String roleDesc) {
        // This is serious, as it means the variant cannot be stored in this genome.
        this.goodVariant = false;
        activateCell(idx);
    }

    /**
     * Denote that the specified role is active in this variant.
     *
     * @param idx	column number (0-based) of the active role
     */
    private void activateCell(int idx) {
        this.variant.setCell(idx, this.getProjector());
    }

    @Override
    protected void recordReplacementFeatures(int idx, String fid, String roleDesc, Set<String> fids) {
        activateCell(idx);
    }

    @Override
    protected void recordIncorrectRole(int idx, Feature feat, String roleDesc) {
        // Again, this means the variant cannot be stored in this genome.  However, we still
        // recognize that if it is found somewhere else, it is a valid variant.
        this.goodVariant = false;
        activateCell(idx);
        log.info("Bad variant {} found due to missing role \"{}\". Original feature was {} ({}).",
                this.variant, roleDesc, feat.getId(), feat.getPegFunction());
    }

    @Override
    protected void terminateRow() {
        // Save the variant specification.
        this.totalCount++;
        boolean isNew = this.getProjector().addVariant(variant);
        if (isNew) this.specCount++;
        if (this.goodVariant) {
            // Here we can store it in the genome.
            Genome genome = this.getGenome();
            this.variant.instantiate(genome, this.getRoleMap());
            this.storedCount++;
            log.debug("Good variant {} stored in {}.", this.variant, genome);
        }
    }

    @Override
    protected void terminateAll() {
        log.info("{} subsystem rows scanned, producing {} variant specifications.  {} genome updates were made.",
                this.totalCount, this.specCount, this.storedCount);
        if (this.projectorFile != null) {
            log.info("Saving subsystem projector to {}.", this.projectorFile);
            try {
                this.getProjector().save(this.projectorFile);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

}
