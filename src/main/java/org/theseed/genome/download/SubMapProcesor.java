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
import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.p3api.Criterion;
import org.theseed.p3api.KeyBuffer;
import org.theseed.p3api.P3Connection;
import org.theseed.p3api.P3Connection.Table;
import org.theseed.subsystems.SubsystemIdMap;

/**
 * This command creates or updates a subsystem map.  A subsystem map is a simple two-column table that assigns an abbreviated
 * ID to each subsystem in PATRIC.
 *
 * The positional parameter is the name of the subsystem map file.  If the file does not exist, it will be created.
 * If the file does exist, a backup copy of the file will be created in the same directory before the file is
 * modified.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --back	file name to use for backup; the default is the input file name suffixed with ".bak"
 *
 * @author Bruce Parrello
 *
 */
public class SubMapProcesor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SubMapProcesor.class);
    /** subsystem ID map */
    private SubsystemIdMap subMap;
    /** PATRIC connection for retrieving current subsystems */
    private P3Connection p3;

    // COMMAND-LINE OPTIONS

    /** name to use for the backup file */
    @Option(name = "--back", metaVar = "subMap.tbl.bak",
            usage = "full name and path of the desired backup file (default based on input file name)")
    private File backupFile;

    /** name of the subsystem map file */
    @Argument(index = 0, metaVar = "subMap.tbl", usage = "name of the subsystem map file", required = true)
    private File inFile;

    @Override
    protected void setDefaults() {
        this.backupFile = null;
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // Do we have an old subsystem file?
        if (this.inFile.exists()) {
            // Yes.  Insure we can read it.
            if (! this.inFile.canRead())
                throw new FileNotFoundException("Subsystem map file " + this.inFile + " is unreadable.");
            // Back it up.  We may need to generate the backup file name.
            if (this.backupFile == null)
                this.backupFile = new File(this.inFile.getParentFile(), this.inFile.getName() + ".bak");
            log.info("Backing up {} to {}.", this.inFile, this.backupFile);
            FileUtils.copyFile(this.inFile, this.backupFile);
            // Now read the current file.
            this.subMap = SubsystemIdMap.load(this.inFile);
        } else {
            // There is no existing map.  We start empty.
            log.info("Creating new subsystem ID map.");
            this.subMap = new SubsystemIdMap();
        }
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // Connect to PATRIC.
        this.p3 = new P3Connection();
        // Get a full list of subsystems.
        log.info("Searching for subsystems in PATRIC.");
        var subRecords = p3.query(Table.SUBSYSTEM, "subsystem_name", Criterion.NE("subsystem_id", "_"));
        log.info("{} subsystem records found.", subRecords.size());
        // Memorize the current number of subsystems.
        int startSize = this.subMap.size();
        // Loop through the records.
        int processed = 0;
        long lastMsg = System.currentTimeMillis();
        for (var subRecord : subRecords) {
            String subName = KeyBuffer.getString(subRecord, "subsystem_name");
            if (subName != null) {
                // Insure this subsystem is in the map.
                this.subMap.findOrInsert(subName);
            }
            processed++;
            if (log.isInfoEnabled() && System.currentTimeMillis() - lastMsg >= 5000) {
                log.info("{} subsystem records processed.  {} added to map.", processed, this.subMap.size() - startSize);
                lastMsg = System.currentTimeMillis();
            }
        }
        int total = this.subMap.size();
        log.info("{} subsystem records processed.  {} subsystems in map, {} new.", processed, total, total - startSize);
        // Now save the map.
        this.subMap.save(this.inFile);
    }

}
