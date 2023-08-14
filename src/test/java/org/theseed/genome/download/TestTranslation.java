/**
 *
 */
package org.theseed.genome.download;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.commons.codec.CharEncoding;
import org.junit.Test;

/**
 * @author Bruce Parrello
 *
 */
public class TestTranslation {

    /**
     * Test string translation.
     *
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testTranslator() throws UnsupportedEncodingException {
        SeedProcessor processor = new SeedProcessor();
        String test = "Glycine <-> Cytosin\\e";
        String fixed = processor.fixSubsystemName(test);
        String decoded = URLDecoder.decode(fixed, CharEncoding.UTF_8.toString());
        assertThat(decoded, equalTo(test));
    }

}
