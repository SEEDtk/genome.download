/**
 *
 */
package org.theseed.genome.download;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.theseed.io.TabbedLineReader;
import org.theseed.sequence.FastaInputStream;
import org.theseed.sequence.MD5Hex;
import org.theseed.utils.BaseProcessor;

/**
 * This command reads FASTA files and computes MD5 checksums.  A master list of genomes can be provided, in which
 * case it will also indicate which FASTA files have the same DNA configuration as a known genome.
 *
 * The positional parameters are the names of the FASTA files to process.  If a directory is specified,
 * all FASTA files in the directory and its subdirectories will be processed.
 *
 * The command-line options are as follows.
 *
 * -h 	display command-line usage
 * -v	display more detailed progress messages
 * -g	the name of a tab-delimited file containing the checksums of known genomes; the genome IDs should be in the
 * 	  	column "genome_id", the genome name in the column "genome_name", and the checksums in the column "md5_checksum"
 *
 * @author Bruce Parrello
 *
 */
public class ChecksumProcessor extends BaseProcessor {

    // FIELDS
    /** hash of checksums to genome_id/name */
    private Map<String, String> checkMap;
    /** MD5 computer */
    private MD5Hex computer;
    /** list of FASTA files to process */
    private List<File> files;
    /** FASTA file name pattern */
    private static final Pattern FASTAFILE = Pattern.compile(".+\\.(?:fa|fasta|faa|fna)$");


    // COMMAND-LINE OPTIONS
    @Option(name = "-g", aliases = { "--genomes" }, metaVar = "patric.master.tbl", usage = "file mapping checksums to genome ID and name")
    private File genomeFile;

    @Argument(index = 0, metaVar = "file1 file2 dir1 ...", usage = "FASTA files and directories to process", multiValued = true)
    private List<File> fastaList;

    @Override
    protected void setDefaults() {
        this.checkMap = new HashMap<String, String>();
        this.files = new ArrayList<File>(100);
        this.genomeFile = null;
    }

    @Override
    protected boolean validateParms() throws IOException {
        if (this.genomeFile != null) {
            log.info("Genome IDs/names will be read from {}.", this.genomeFile);
            // Read all the genomes into the hash.
            try (TabbedLineReader genomeStream = new TabbedLineReader(this.genomeFile)) {
                int idCol = genomeStream.findField("genome_id");
                int nameCol = genomeStream.findField("genome_name");
                int md5Col = genomeStream.findField("md5_checksum");
                for (TabbedLineReader.Line line : genomeStream)
                    this.checkMap.put(line.get(md5Col), line.get(idCol) + "\t" + line.get(nameCol));
                log.info("{} genomes read into checksum map.", this.checkMap.size());
            }
        }
        // Now we need to process the input files.  All files are transferred into the file list directly,
        // and directories are walked.
        for (File fastaFile : this.fastaList) {
            if (fastaFile.isFile()) {
                   this.files.add(fastaFile);
            } else if (fastaFile.isDirectory()) {
                try (Stream<Path> walk = Files.walk(fastaFile.toPath())) {
                    List<File> found = walk.map(x -> x.toFile()).filter(f -> f.isFile() && FASTAFILE.matcher(f.toString()).matches())
                            .collect(Collectors.toList());
                    log.info("{} FASTA files found in {}.", found.size(), fastaFile);
                    this.files.addAll(found);
                }
            } else {
                throw new FileNotFoundException(fastaFile + " is not found or invalid.");
            }
        }
        log.info("{} files selected for MD5 analysis.", this.files.size());
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        this.computer = new MD5Hex();
        // Everything is ready at this point.  We start the output, then we loop through the FASTA files computing MD5s.
        System.out.println("file\tmd5_checksum\tgenome_id\tgenome_name");
        for (File fastaFile : this.files) {
            try (FastaInputStream fastaStream = new FastaInputStream(fastaFile)) {
                String checksum = this.computer.sequenceMD5(fastaStream);
                String genomeSpec = this.checkMap.getOrDefault(checksum, "");
                System.out.format("%s\t%s\t%s%n", fastaFile.toString(), checksum, genomeSpec);
            }
        }
    }

}
