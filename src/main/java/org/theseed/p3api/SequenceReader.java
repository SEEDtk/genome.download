/**
 *
 */
package org.theseed.p3api;

import java.util.Collection;
import org.theseed.sequence.Sequence;

/**
 * This is the base class for reading sequences of a genome from PATRIC.  The client specifies a connection
 * to PATRIC and then individual genomes can be downloaded.
 *
 * @author Bruce Parrello
 *
 */
public abstract class SequenceReader {

    // FIELDS
    /** connection to PATRIC */
    private final P3Connection p3;

    /**
     * This enumeration describes the different sequence reader types.
     */
    public static enum Type {
        /** DNA FASTA for contigs */
        CONTIGS {
            @Override
            public SequenceReader create(P3Connection p3) {
                return new ContigSequenceReader(p3);
            }
        },
        /** protein FASTA for coding features */
        PEGS {
            @Override
            public SequenceReader create(P3Connection p3) {
                return new PegSequenceReader(p3);
            }
        },
        /** DNA FASTA for all features */
        FEATURES {
            @Override
            public SequenceReader create(P3Connection p3) {
                return new GeneSequenceReader(p3);
            }
        };

        /**
         * @return a sequence reader of this type.
         *
         * @param p3	connection to PATRIC
         */
        public abstract SequenceReader create(P3Connection p3);

    }

    /**
     * Construct a new sequence reader for the specified genome, using the specified PATRIC connection.
     *
     * @param p3		connection to PATRIC
     */
    public SequenceReader(P3Connection p3) {
        this.p3 = p3;
    }

    /**
     * Read all the sequences for the specified genome.
     *
     * @param genomeId	ID of the genome to download
     *
     * @return a collection of the sequences for that genome
     */
    public Collection<Sequence> getSequences(String genomeId) {
        Collection<Sequence> retVal = this.download(p3, genomeId);
        return retVal;
    }

    /**
     * Download the specified genome's sequence from PATRIC.
     *
     * @param p3		connection to PATRIC
     * @param genomeId	ID of the genome to download
     *
     * @return a collection of the sequences of the specified type, downloaded from PATRIC
     */
    protected abstract Collection<Sequence> download(P3Connection p32, String genomeId);

    /**
     * @return the filename extension to use for FASTA files of this type
     */
    public abstract String getExtension();

}
