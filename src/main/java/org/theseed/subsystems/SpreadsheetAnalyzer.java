/**
 *
 */
package org.theseed.subsystems;

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Feature;
import org.theseed.genome.Genome;

/**
 * This is the base class for subsystem spreadsheet analysis.  It provides access to the subsystem
 * projector and the current genome of interest, and allows hooks for recording key events in the
 * spreadsheet analysis.  It is called at various points by the SubsystemProcessor to perform the
 * different kinds of tracking desired.
 *
 * @author Bruce Parrello
 *
 */
public abstract class SpreadsheetAnalyzer {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SpreadsheetAnalyzer.class);
    /** current genome */
    private Genome genome;
    /** current subsystem */
    private SubsystemSpec subsystem;
    /** subsystem projector */
    private SubsystemProjector projector;
    /** genome role map, connecting each role ID to the features with that role */
    private Map<String, Set<String>> roleMap;

    /**
     * Construct a new spreadsheet analyzer.  All analyzers must be constructed with an active projector.
     *
     * @param projector		relevant subsystem projector
     */
    public SpreadsheetAnalyzer(SubsystemProjector projector) {
        this.projector = projector;
    }

    /**
     * Register the current subsystem row.
     *
     * @param genome		genome of interest
     * @param roleMap		genome role map
     * @param subsystem		subsystem in the genome
     * @param variantCode	relevant variant code
     */
    public void openRow(Genome genome, Map<String, Set<String>> roleMap, SubsystemSpec subsystem, String variantCode) {
        this.genome = genome;
        this.subsystem = subsystem;
        this.roleMap = roleMap;
        this.initializeRow(variantCode);
    }

    /**
     * Perform initialization processing for a subsystem row.
     *
     * @param variantCode	relevant variant code
     */
    protected abstract void initializeRow(String variantCode);

    /**
     * Denote this is a good cell.  That is, the role and feature match.
     *
     * @param idx	column index (0-based) of the good cell
     * @param feat	feature in the cell
     */
    public void goodCell(int idx, Feature feat) {
        this.recordGoodCell(idx, feat);
    }

    /**
     * Process a correctly-annotated feature in a cell.
     *
     * @param idx	column index (0-based) of the cell
     * @param feat	feature in the cell
     */
    protected abstract void recordGoodCell(int idx, Feature feat);

    /**
     * Denote this cell has a missing feature.
     *
     * @param idx		column index (0-based) of the cell
     * @param fid		ID of the missing feature
     * @param roleDesc	description of the role expected in the cell
     */
    public void badCell(int idx, String fid, String roleDesc) {
        this.recordMissingFeature(idx, fid, roleDesc);
    }

    /**
     * Process a missing feature in a cell.
     *
     * @param idx		column index (0-based) of the cell
     * @param fid		ID of the missing feature
     * @param roleDesc	description of the role expected in the cell
     */
    protected abstract void recordMissingFeature(int idx, String fid, String roleDesc);

    /**
     * Denote this cell has a replaceable feature.
     *
     * @param idx		column index (0-based) of the cell
     * @param fid		ID of the missing feature
     * @param roleDesc	description of the role expected in the cell
     * @param fids		set of replacement features
     */
    public void badCell(int idx, String fid, String roleDesc, Set<String> fids) {
        this.recordReplacementFeatures(idx, fid, roleDesc, fids);
    }

    /**
     * Process replacement features for a cell.
     *
     * @param idx		column index (0-based) of the cell
     * @param fid		ID of the missing feature
     * @param roleDesc	description of the role expected in the cell
     * @param fids		set of replacement features
     */
    protected abstract void recordReplacementFeatures(int idx, String fid, String roleDesc, Set<String> fids);

    /**
     * Denote this cell has an invalid role.
     *
     * @param idx		column index (0-based) of the cell
     * @param feat		feature containing the invalid role
     * @param roleDesc	description of the role expected in the cell
     */
    public void badCell(int idx, Feature feat, String roleDesc) {
        this.recordIncorrectRole(idx, feat, roleDesc);
    }

    /**
     * Denote this cell has an invalid role.
     *
     * @param idx		column index (0-based) of the cell
     * @param feat		feature containing the invalid role
     * @param roleDesc	description of the role expected in the cell
     */
    protected abstract void recordIncorrectRole(int idx, Feature feat, String roleDesc);

    /**
     * Finish processing this spreadsheet row.
     */
    public void closeRow() {
        this.terminateRow();
    }

    /**
     * Produce final output for this spreadsheet row.
     */
    protected abstract void terminateRow();

    /**
     * Finish all processing.
     */
    public void close() {
        this.terminateAll();
    }

    /**
     * Produce final output for the entire analysis.
     */
    protected abstract void terminateAll();

    /**
     * @return the current genome
     */
    protected Genome getGenome() {
        return genome;
    }

    /**
     * @return the current subsystem
     */
    protected SubsystemSpec getSubsystem() {
        return subsystem;
    }

    /**
     * @return the subsystem projector
     */
    protected SubsystemProjector getProjector() {
        return projector;
    }

    /**
     * @return the current role-to-features map
     */
    protected Map<String, Set<String>> getRoleMap() {
        return roleMap;
    }


}
