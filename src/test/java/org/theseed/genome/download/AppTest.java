package org.theseed.genome.download;

import junit.framework.Test;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.commons.codec.CharEncoding;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Test string translation.
     *
     * @throws UnsupportedEncodingException
     */
    public void testTranslator() throws UnsupportedEncodingException {
        SeedProcessor processor = new SeedProcessor();
        String test = "Glycine <-> Cytosin\\e";
        String fixed = processor.fixSubsystemName(test);
        String decoded = URLDecoder.decode(fixed, CharEncoding.UTF_8.toString());
        assertThat(decoded, equalTo(test));
    }

}
