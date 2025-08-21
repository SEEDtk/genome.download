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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.counters.CountMap;
import org.theseed.genome.Feature;
import org.theseed.subsystems.core.SubsystemDescriptor;

/**
 * This analyzer counts the number of times each role is found in a spreadsheet cell, and produces a summary
 * report.  The report will only be produced for cells where an incorrect role is found.
 *
 * @author Bruce Parrello
 *
 */
public class CountingSpreadsheetAnalyzer extends SpreadsheetAnalyzer {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(CountingSpreadsheetAnalyzer.class);
    /** output file for report */
    private PrintWriter outStream;
    /** map of subsystems to cell specifiers */
    private final Map<SubsystemDescriptor, CellSpec[]> countingMap;
    /** currently-active cell-specification array */
    private CellSpec[] currentCounts;
    /** number of variants with incorrect roles */
    private int badVariants;
    /** TRUE if the current variant has incorrect roles */
    private boolean badRoles;
    /** heading columns for report */
    private final String TRACKING_HEADER = "Subsystem\tCell_Index\tRole\tCount";
    /** format for report lines */
    private final String TRACKING_FORMAT = "%s\t%d%c\t%s\t%d%n";

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
        private final CountMap<String> badCounts;

        /**
         * Create a new cell counter.
         *
         * @param idx	column index for relevant cell
         */
        private CellSpec() {
            this.goodCount = 0;
            this.totalCount = 0;
            this.badCounts = new CountMap<>();
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


    /**
     * Set up to produce the role-tracking report.
     *
     * @param projector		controlling subsystem projector
     * @param outFile		target output file
     */
    public CountingSpreadsheetAnalyzer(File outFile) {
        this.badVariants = 0;
        // Set up the output file.
        log.info("Role counts will be output to {}.", outFile);
        try {
            this.outStream = new PrintWriter(outFile);
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException(e);
        }
        this.outStream.println(TRACKING_HEADER);
        // Create the count map.  Note we sort by subsystem name.
        this.countingMap = new TreeMap<>();
    }

    @Override
    protected void initializeRow(String variantCode) {
        this.badRoles = false;
        // Verify that we have the cell-specification array for the current subsystem.
        SubsystemDescriptor subsystem = this.getSubsystem();
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
        this.currentCounts[idx].countBad(feat.getFunction());
        this.badRoles = true;
    }

    @Override
    protected void terminateRow() {
        if (this.badRoles) this.badVariants++;
    }

    @Override
    protected void terminateAll() {
        int badSubsystems = 0;
        int badColumns = 0;
        // Now we have accumulated all the counts and we spool them off by subsystem.
        for (Map.Entry<SubsystemDescriptor, CellSpec[]> subEntry : this.countingMap.entrySet()) {
            SubsystemDescriptor subsystem = subEntry.getKey();
            boolean badSubsystem = false;
            CellSpec[] subCounts = subEntry.getValue();
            for (int i = 0; i < subCounts.length; i++)
                if (subCounts[i].isReportable()) {
                    // Here we have a subsystem cell with data we want to report.
                    badSubsystem = true;
                    badColumns++;
                    // First we do the good role.
                    this.outStream.format(TRACKING_FORMAT, subsystem.getName(), i, '*',
                            subsystem.getRole(i), subCounts[i].goodCount);
                    // Now the bad roles, from most to least frequent.
                    for (CountMap<String>.Count count : subCounts[i].badCounts.sortedCounts())
                        this.outStream.format(TRACKING_FORMAT, subsystem.getName(), i, ' ',
                                count.getKey(), count.getCount());
                }
            if (badSubsystem) badSubsystems++;
        }
        log.info("{} variants with incorrect roles found in {} subsystems.", this.badVariants, badSubsystems);
        log.info("{} subsystem columns require review.", badColumns);
        // Close the output file.
        this.outStream.close();
    }

}
