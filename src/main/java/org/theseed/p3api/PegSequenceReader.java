/**
 *
 */
package org.theseed.p3api;

/**
 * This subclass outputs the protein sequences for pegs in genomes.
 *
 * @author Bruce Parrello
 *
 */
public class PegSequenceReader extends FeatureSequenceReader {

    public PegSequenceReader(P3Connection p3) {
        super(p3);
    }

    @Override
    protected String getSequenceField() {
        return "aa_sequence_md5";
    }

    @Override
    protected String formatSequence(String string) {
        return string.toUpperCase();
    }

    @Override
    public String getExtension() {
        return "faa";
    }

}
