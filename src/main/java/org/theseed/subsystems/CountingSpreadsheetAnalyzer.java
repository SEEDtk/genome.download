/**
 *
 */
package org.theseed.subsystems;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.theseed.counters.CountMap;
import org.theseed.genome.Feature;

/**
 * This analyzer counts the number of times each role is found in a spreadsheet cell, and produces a summary
 * report.  The report will only be produced for cells where an incorrect role is found.
 *
 * @author Bruce Parrello
 *
 */
public class CountingSpreadsheetAnalyzer extends SpreadsheetAnalyzer {

    /**
     * This object represents the roles found in a spreadsheet cell.  It counts the number of good
     * roles, and the number of occurrences for each incorrect role.
     */
    private static class CellSpec {
        /** number of features with the correct role */
        private int goodCount;
        /** total number of features */
        private int totalCount;
        /** number of features for each incorrect role */
        private CountMap<String> badCounts;

        /**
         * Create a new cell counter.
         *
         * @param idx	column index for relevant cell
         */
        private CellSpec() {
            this.goodCount = 0;
            this.totalCount = 0;
            this.badCounts = new CountMap<String>();
        }

        /**
         * Count a good role occurrence.
         */
        private void countGood() {
            this.goodCount++;
            this.totalCount++;
        }

        /**
         * Count a bad role occurrence.
         *
         * @param badRole	functional assignment found
         */
        private void countBad(String roleDesc) {
            this.totalCount++;
            this.badCounts.count(roleDesc);
        }

        /**
         * @return TRUE if there are bad roles to report
         */
        private boolean isReportable() {
            return (this.goodCount < this.totalCount);
        }

    }

    // FIELDS
    /** output file for report */
    private PrintWriter outStream;
    /** map of subsystems to cell specifiers */
    private Map<SubsystemSpec, CellSpec[]> countingMap;
    /** currently-active cell-specification array */
    private CellSpec[] currentCounts;
    /** heading columns for report */
    private final String TRACKING_HEADER = "Subsystem\tCell_Index\tRole\tCount";
    /** format for report lines */
    private final String TRACKING_FORMAT = "%s\t%%d%c\t%s\t%d%n";

    /**
     * Set up to produce the role-tracking report.
     *
     * @param projector		controlling subsystem projector
     * @param outFile		target output file
     */
    public CountingSpreadsheetAnalyzer(SubsystemProjector projector, File outFile) {
        super(projector);
        // Set up the output file.
        log.info("Role counts will be output to {}.", outFile);
        try {
            this.outStream = new PrintWriter(outFile);
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException(e);
        }
        this.outStream.println(TRACKING_HEADER);
        // Create the count map.  Note we sort by subsystem name.
        this.countingMap = new TreeMap<SubsystemSpec, CellSpec[]>();
    }

    @Override
    protected void initializeRow(String variantCode) {
        // Verify that we have the cell-specification array for the current subsystem.
        SubsystemSpec subsystem = this.getSubsystem();
        if (! this.countingMap.containsKey(subsystem)) {
            int roleCount = subsystem.getRoleCount();
            this.currentCounts = new CellSpec[roleCount];
            for (int i = 0; i < roleCount; i++)
                this.currentCounts[i] = new CellSpec();
            this.countingMap.put(subsystem, this.currentCounts);
         } else {
             this.currentCounts = this.countingMap.get(subsystem);
         }
    }

    @Override
    protected void recordGoodCell(int idx, Feature feat) {
        this.currentCounts[idx].countGood();
    }

    @Override
    protected void recordMissingFeature(int idx, String fid, String roleDesc) {
    }

    @Override
    protected void recordReplacementFeatures(int idx, String fid, String roleDesc, Set<String> fids) {
        // This counts as good:  the role is there, it is just in a different feature.
        this.currentCounts[idx].countGood();
    }

    @Override
    protected void recordIncorrectRole(int idx, Feature feat, String roleDesc) {
        // This is the genuine wrong-role case.
        this.currentCounts[idx].countBad(roleDesc);
    }

    @Override
    protected void terminateRow() {
    }

    @Override
    protected void terminateAll() {
        // Now we have accumulated all the counts and we spool them off by subsystem.
        for (Map.Entry<SubsystemSpec, CellSpec[]> subEntry : this.countingMap.entrySet()) {
            SubsystemSpec subsystem = subEntry.getKey();
            CellSpec[] subCounts = subEntry.getValue();
            for (int i = 0; i < subCounts.length; i++)
                if (subCounts[i].isReportable()) {
                    // Here we have a subsystem cell with data we want to report.
                    // First we do the good role.
                    this.outStream.format(TRACKING_FORMAT, subsystem.getName(), i, '*',
                            subsystem.getRole(i), subCounts[i].goodCount);
                    // Now the bad roles, from most to least frequent.
                    for (CountMap<String>.Count count : subCounts[i].badCounts.sortedCounts())
                        this.outStream.format(TRACKING_FORMAT, subsystem.getName(), i, ' ',
                                count.getKey(), count.getCount());
                }
        }
        // Close the output file.
        this.outStream.close();
    }

}
