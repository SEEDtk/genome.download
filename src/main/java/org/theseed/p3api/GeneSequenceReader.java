/**
 *
 */
package org.theseed.p3api;

/**
 * This subclass outputs the DNA sequences of features in genomes.
 *
 * @author Bruce Parrello
 *
 */
public class GeneSequenceReader extends FeatureSequenceReader {

    public GeneSequenceReader(P3Connection p3) {
        super(p3);
    }

    @Override
    protected String getSequenceField() {
        return "na_sequence_md5";
    }

    @Override
    protected String formatSequence(String string) {
        return string.toLowerCase();
    }

    @Override
    public String getExtension() {
        return "fna";
    }

}
