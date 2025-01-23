/**
 *
 */
package org.theseed.subsystems;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;


import org.theseed.genome.Feature;
import org.theseed.subsystems.core.SubsystemDescriptor;

/**
 * This analyzer finds bad spreadsheet rows and missing roles.
 *
 * @author Bruce Parrello
 *
 */
public class ProjectionSpreadsheetAnalyzer extends SpreadsheetAnalyzer {

    // FIELDS
    /** current variant specification */
    private VariantDef variant;
    /** TRUE if this variant can be projected */
    private boolean goodVariant;
    /** number of rows processed */
    private int totalCount;
    /** number of good rows found */
    private int goodRowCount;
    /** number of distinct specifications */
    private int specCount;
    /** set of distinct variant specifications found */
    private Set<VariantDef> variants;

    /**
     * This object describes a variant.
     */
    protected static class VariantDef {

        /** parent subsystem name */
        private String subsysName;
        /** variant code */
        private String vCode;
        /** map of present roles */
        private BitSet roles;

        protected VariantDef(SubsystemDescriptor subsys, String variantCode) {
            this.subsysName = subsys.getName();
            this.vCode = variantCode;
            this.roles = new BitSet(subsys.getRoleCount());
        }

        /**
         * Specify that a spreadsheet cell is active in this variant.
         *
         * @param idx	column index of active cell.
         */
        public void setCell(int idx) {
            this.roles.set(idx);
        }

        @Override
        public int hashCode() {
            return Objects.hash(roles, subsysName, vCode);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            VariantDef other = (VariantDef) obj;
            return Objects.equals(this.roles, other.roles) && Objects.equals(this.subsysName, other.subsysName)
                    && Objects.equals(this.vCode, other.vCode);
        }

    }

    /**
     * Set up to write out a completed subsystem projector.
     *
     * @param projector		subsystem projector being constructed
     * @param outFile		designated output file
     */
    public ProjectionSpreadsheetAnalyzer() {
        // Clear the counters.
        this.totalCount = 0;
        this.goodRowCount = 0;
        this.specCount = 0;
        // Set up the variant specification set.
        this.variants = new HashSet<VariantDef>(4000);
    }

    @Override
    protected void initializeRow(String variantCode) {
        // Create the variant specification we are building from this row.
        this.variant = new VariantDef(this.getSubsystem(), variantCode);
        this.goodVariant = true;
    }

    @Override
    protected void recordGoodCell(int idx, Feature feat) {
        this.activateCell(idx);
    }

    @Override
    protected void recordMissingFeature(int idx, String fid, String roleDesc) {
        // This is serious, as it means the variant cannot be stored in this genome.
        this.goodVariant = false;
        this.activateCell(idx);
    }

    /**
     * Denote that the specified role is active in this variant.
     *
     * @param idx	column number (0-based) of the active role
     */
    private void activateCell(int idx) {
        this.variant.setCell(idx);
    }

    @Override
    protected void recordReplacementFeatures(int idx, String fid, String roleDesc, Set<String> fids) {
        this.activateCell(idx);
    }

    @Override
    protected void recordIncorrectRole(int idx, Feature feat, String roleDesc) {
        // Again, this means the variant cannot be stored in this genome.  However, we still
        // recognize that if it is found somewhere else, it is a valid variant.
        this.goodVariant = false;
        this.activateCell(idx);
        log.info("Bad variant {} found due to missing role \"{}\". Original feature was {} ({}).",
                this.variant, roleDesc, feat.getId(), feat.getPegFunction());
    }

    @Override
    protected void terminateRow() {
        // Save the variant specification.
        this.totalCount++;
        boolean isNew = this.variants.add(variant);
        if (isNew) this.specCount++;
        if (this.goodVariant) {
            // Here we have a good spreadsheet row.
            this.goodRowCount++;
        }
    }

    @Override
    protected void terminateAll() {
        log.info("{} subsystem rows scanned, producing {} variant specifications.  {} good rows found.",
                this.totalCount, this.specCount, this.goodRowCount);
    }

}
