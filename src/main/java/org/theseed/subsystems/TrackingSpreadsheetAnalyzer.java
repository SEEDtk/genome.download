/**
 *
 */
package org.theseed.subsystems;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.theseed.genome.Feature;

/**
 * This analyzer produces a report on spreadsheet cells that need to be updated.
 *
 * @author Bruce Parrello
 *
 */
public class TrackingSpreadsheetAnalyzer extends SpreadsheetAnalyzer {

    // FIELDS
    /** output stream for report */
    private PrintWriter outStream;
    /** current variant code */
    private String varCode;
    /** number of errors in the current row */
    private int errorCount;
    /** heading line for the output */
    private static final String ERROR_STREAM_HEADER = "Subsystem\tGenome\tVariant\tColumn\tFeature\tExpected Role\tReplacements\tFound Role";
    /** format string for the output */
    private static final String ERROR_STREAM_FORMAT = "%s\t%s\t%s\t%4d\t%s\t%s\t%s\t%s%n";

    /**
     * Set up to produce the tracking report.
     *
     * @param projector		controlling subsystem projector
     * @param outFile		output file for the report
     */
    public TrackingSpreadsheetAnalyzer(SubsystemProjector projector, File outFile) {
        super(projector);
        log.info("Error-tracking output will be to {}.", outFile);
        try {
            this.outStream = new PrintWriter(outFile);
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException(e);
        }
        this.outStream.println(ERROR_STREAM_HEADER);
    }

    @Override
    protected void initializeRow(String variantCode) {
        this.varCode = variantCode;
        this.errorCount = 0;
    }

    @Override
    protected void recordGoodCell(int idx, Feature feat) {
    }

    @Override
    protected void recordMissingFeature(int idx, String fid, String roleDesc) {
        this.writeRow(idx, fid, roleDesc, "", "");
    }

    /**
     * Write a row of error output.
     *
     * @param idx			column containing the error
     * @param fid			ID of the incorrect feature
     * @param roleDesc		expected role description
     * @param fids			IDs of the correct features for the cell
     * @param roleFound		actual role description
     */
    private void writeRow(int idx, String fid, String roleDesc, String fids, String roleFound) {
        this.outStream.format(ERROR_STREAM_FORMAT, this.getSubsystem().getName(), this.varCode,
                idx, fid, roleDesc, fids, roleFound);
        this.errorCount++;
    }

    @Override
    protected void recordReplacementFeatures(int idx, String fid, String roleDesc, Set<String> fids) {
        String fidsList = StringUtils.join(fids, ", ");
        this.writeRow(idx, fid, roleDesc, fidsList, "");
    }

    @Override
    protected void recordIncorrectRole(int idx, Feature feat, String roleDesc) {
        this.writeRow(idx, feat.getId(), roleDesc, "", feat.getFunction());
    }

    @Override
    protected void terminateRow() {
        if (this.errorCount > 0)
            log.info("{} errors found in {} for subsystem {}.", this.errorCount, this.getGenome(),
                    this.getSubsystem());
    }

    @Override
    protected void terminateAll() {
        this.outStream.close();
    }

}
