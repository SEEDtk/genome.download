/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Genome;
import org.theseed.genome.GffGenome;
import org.theseed.utils.BaseProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This command is used to create a GTO from files downloaded from NCBI.  The positional parameters are a GFF
 * file and a nucleotide FASTA file downloaded from the main genome page.
 *
 * The command-line options are as follows.
 *
 * -h	display usage
 * -v	show more detailed progress messages
 * -d	output directory to contain the GTO file; the default is the current directory
 *
 * --suffix		suffix string to add to the genome name
 * --domain		domain of the genome (Virus, Eukaryota, Bacteria, etc)
 *
 * @author Bruce Parrello
 *
 */
public class GffProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(GffProcessor.class);

    // COMMAND-LINE OPTIONS

    /** output directory */
    @Option(name = "-d", aliases = { "--dir" }, usage = "directory to contain output GTO")
    private File outDir;

    /** name suffix */
    @Option(name = "--suffix", metaVar = "loaded from NCBI", usage = "suffix for genome name")
    private String nameSuffix;

    /** genome domain */
    @Option(name = "--domain", metaVar = "Eukaryota", usage = "domain of the genome")
    private String domain;

    /** GFF file name */
    @Argument(index = 0, metaVar = "genomeFile.gff", usage = "GFF file for the genome", required = true)
    private File gffFile;

    /** FASTA file name */
    @Argument(index = 1, metaVar = "contigs.fna", usage = "nucleic acid FASTA file for the genome", required = true)
    private File fastaFile;

    @Override
    protected void setDefaults() {
        this.outDir = new File(System.getProperty("user.dir"));
        this.nameSuffix = "";
        this.domain = "Bacteria";
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // Insure the domain is valid.
        switch (this.domain) {
        case "Bacteria" :
        case "Archaea" :
        case "Eukaryota" :
        case "Virus" :
            log.info("Genome domain is {}.", this.domain);
            break;
        default :
            throw new ParseFailureException("Invalid domain " + this.domain + ".");
        }
        // Check the files.
        if (! this.gffFile.canRead())
            throw new FileNotFoundException("GFF file " + this.gffFile + " not found or unreadable.");
        if (! this.fastaFile.canRead())
            throw new FileNotFoundException("FASTA file " + this.fastaFile + " not found or unreadable.");
        if (! this.outDir.isDirectory()) {
            log.info("Creating output directory {}.", this.outDir);
            FileUtils.forceMkdir(this.outDir);
        }
        return true;
    }

    @Override
    public void runCommand() throws Exception {
        Genome genome = new GffGenome(this.nameSuffix, this.domain, this.gffFile, this.fastaFile);
        File outFile = new File(this.outDir, genome.getId() + ".gto");
        log.info("Saving genome {} to {}.", genome, outFile);
        genome.save(outFile);
    }

}
