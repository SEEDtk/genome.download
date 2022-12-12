/**
 *
 */
package org.theseed.p3api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.theseed.p3api.P3Connection.Table;
import org.theseed.sequence.Sequence;

/**
 * This is the base class for methods that read feature sequences.  The difference between the two types (PEGS and
 * FEATURES) is whether we get the sequence ID from "na_sequence_md5" or "aa_sequence_md5" and whether we convert to lower
 * case or upper case.
 *
 * @author Bruce Parrello
 *
 */
public abstract class FeatureSequenceReader extends SequenceReader {

    // FIELDS
    /** field list to use for feature table query */
    private String fieldList;
    /** sequence field ID in feature table */
    private String seqField;
    /** map of sequence IDs to skeleton feature sequences */
    private Map<String, List<Sequence>> seqIdMap;
    /** size of a sequence query batch */
    private static final int BATCH_SIZE = 399;

    /**
     * Create a new feature sequence reader.
     *
     * @param p3	connection to PATRIC
     */
    public FeatureSequenceReader(P3Connection p3) {
        super(p3);
        // Compute the list of fields to use in feature queries.
        this.seqField = this.getSequenceField();
        this.fieldList = "patric_id,product," + this.seqField;
        // Create the holder for the sequence ID batch.
        this.seqIdMap = new HashMap<String, List<Sequence>>(BATCH_SIZE * 4 / 3 + 1);
    }

    /**
     * @return the name of the feature field containing the sequence ID.
     */
    protected abstract String getSequenceField();

    @Override
    protected Collection<Sequence> download(P3Connection p3, String genomeId) {
        // The process is two steps.  First, we get all the features.  Then, we read the actual sequences in batches.
        var features = p3.getRecords(Table.FEATURE, "genome_id", Collections.singleton(genomeId), fieldList,
                Criterion.EQ("annotation", "PATRIC"));
        // We will accumulate sequences in here.
        List<Sequence> retVal = new ArrayList<Sequence>(features.size());
        // The basic strategy is to collect a batch of features and then query the sequence table.
        this.seqIdMap.clear();
        for (var feature : features) {
            // Insure there is room in the batch.
            if (this.seqIdMap.size() >= BATCH_SIZE) {
                this.processBatch(p3, retVal);
                this.seqIdMap.clear();
            }
            String label = P3Connection.getString(feature, "patric_id");
            String comment = P3Connection.getString(feature, "product");
            String seqId = P3Connection.getString(feature, this.seqField);
            // Only proceed if the sequence ID is valid.
            if (! StringUtils.isBlank(seqId)) {
                List<Sequence> seqList = this.seqIdMap.computeIfAbsent(seqId, x -> new ArrayList<Sequence>(5));
                seqList.add(new Sequence(label, comment, ""));
            }
        }
        // Output the residual batch.
        if (! this.seqIdMap.isEmpty())
            this.processBatch(p3, retVal);
        // Return the list.
        return retVal;
    }

    /**
     * Get the sequences for the current batch and add them to the specified output list.
     *
     * @param p3			connection to PATRIC
     * @param outputList	list of sequences to be used for output
     */
    private void processBatch(P3Connection p3, List<Sequence> outputList) {
        // Get all of the sequences in the batch.
        var seqRecords = p3.getRecords(Table.SEQUENCE, this.seqIdMap.keySet(), "sequence");
        // For each sequence string, format and output its FASTA sequence objects.
        for (var seqEntry : seqRecords.entrySet()) {
            String seqId = seqEntry.getKey();
            var seqRecord = seqEntry.getValue();
            // Get the actual sequence and post-process it.
            var sequence = this.formatSequence(P3Connection.getString(seqRecord, "sequence"));
            // If it id valid, output its FASTA records.
            var seqList = this.seqIdMap.get(seqId);
            for (Sequence seq : seqList) {
                seq.setSequence(sequence);
                outputList.add(seq);
            }
        }

    }

    /**
     * @return the output format of the specified sequence string
     *
     * @param string		sequence string to format
     */
    protected abstract String formatSequence(String string);

}
