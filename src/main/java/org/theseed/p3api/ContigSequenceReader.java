/**
 *
 */
package org.theseed.p3api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.theseed.p3api.P3Connection.Table;
import org.theseed.sequence.Sequence;

/**
 * Download the contig DNA sequences from the specified genome.
 *
 * @author Bruce Parrello
 *
 */
public class ContigSequenceReader extends SequenceReader {

    /**
     * Construct a contig sequence reader.
     *
     * @param p3		connection to PATRIC
     */
    public ContigSequenceReader(P3Connection p3) {
        super(p3);
    }

    @Override
    protected Collection<Sequence> download(P3Connection p3, String genomeId) {
        // This is a fairly simple process, since all the contig information is in the CONTIG table.
        var contigs = p3.getRecords(Table.CONTIG, "genome_id", Collections.singleton(genomeId), "sequence_id,description,sequence");
        List<Sequence> retVal = new ArrayList<Sequence>(contigs.size());
        // Loop through the contigs, collecting sequences.
        for (var contig : contigs) {
            // Insure DNA is output in lower case.  Note that getString never returns NULL, only empty strings.
            String sequence = P3Connection.getString(contig, "sequence").toLowerCase();
            // Skip the sequence if it is blank.
            if (! sequence.isEmpty()) {
                // Get the label and the comment.  For genomes created from read assemblies, the description is the original contig ID.
                // Note we prefix the genome ID to the contig ID to insure uniqueness if the output groups are combined.
                String label = genomeId + ":" + P3Connection.getString(contig, "sequence_id");
                String comment = P3Connection.getString(contig, "description");
                // Output the sequence.
                retVal.add(new Sequence(label, comment, sequence));
            }
        }
        return retVal;
    }

    @Override
    public String getExtension() {
        return "fasta";
    }

}
