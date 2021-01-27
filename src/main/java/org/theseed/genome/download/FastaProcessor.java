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
import org.theseed.genome.FastaGenome;
import org.theseed.genome.Genome;
import org.theseed.utils.BaseProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This command converts FASTA files into skeleton GTOs.
 *
 * The positional parameters are the taxonomic ID and the FASTA input file name.
 *
 * The command-line options are as follows.
 *
 * -h	display command usage
 * -v	display more detailed progress messages
 * -s	name suffix for the genome (default none)
 * -d	output directory for the GTO file (default current directory)
 *
 * --domain	domain of the genome (default "Bacteria")
 *
 * @author Bruce Parrello
 *
 */
public class FastaProcessor extends BaseProcessor {

    // FIELDS
    protected static Logger log = LoggerFactory.getLogger(FastaProcessor.class);

    // COMMAND-LINE OPTIONS

    /** output directory */
    @Option(name = "-d", aliases = { "--dir" }, usage = "directory to contain output GTO")
    private File outDir;

    /** name suffix */
    @Option(name = "--suffix", metaVar = "loaded from FASTA", usage = "suffix for genome name")
    private String nameSuffix;

    /** genome domain */
    @Option(name = "--domain", metaVar = "Virus", usage = "domain of the genome")
    private String domain;

    /** taxonomic ID of the genome */
    @Argument(index = 0, metaVar = "100226", usage = "taxonomic ID of the genome", required = true)
    private int taxonId;

    /** FASTA input file */
    @Argument(index = 1, metaVar = "contigs.fasta", usage = "FASTA file containing contigs", required = true)
    private File fastaFile;

    @Override
    protected void setDefaults() {
        this.domain = "Bacteria";
        this.nameSuffix = "";
        this.outDir = new File(System.getProperty("user.dir"));
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
        // Insure the file is valid.
        if (! this.fastaFile.canRead())
            throw new FileNotFoundException("FASTA file " + this.fastaFile + " is not found or unreadable.");
        if (! this.outDir.isDirectory()) {
            log.info("Creating output directory {}.", this.outDir);
            FileUtils.forceMkdir(this.outDir);
        }
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        Genome genome = new FastaGenome(Integer.toString(this.taxonId), this.nameSuffix, this.domain,
                this.fastaFile);
        log.info("Genome {} loaded from {}.", genome, this.fastaFile);
        File outFile = new File(this.outDir, genome.getId() + ".gto");
        genome.update(outFile);
        log.info("Genome written to {}.", outFile);
    }

}
