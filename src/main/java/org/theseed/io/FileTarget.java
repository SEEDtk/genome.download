/**
 *
 */
package org.theseed.io;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This object represents a destination for a hierarchy of files in folders.  It can either be the
 * file system or an archive stream.
 * 
 * The subclass must provide a default file name if the user does not specify one. The methods "getBaseDir" and "getBaseName" provide
 * the current working directory and a dated file base name to help this process.
 *
 * @author Bruce Parrello
 *
 */
public abstract class FileTarget implements AutoCloseable {

    // FIELDS
    /** logging facility */
    protected static final Logger log = LoggerFactory.getLogger(FileTarget.class);
    /** copy counter */
    private int copyCount;
    /** base name for default file name computation */
    private final String baseName;
    /** base directory for default file name computation */
    private final File baseDir;

    /**
     * This interface describes the parameters required for any controlling command processor
     * that uses a file target.
     */
    public interface IParms {

        /**
         * @return TRUE if the output directory should be erased before processing
         */
        public boolean shouldErase();

    }

    /**
     * This enum describes the different types of file targets.
     */
    public static enum Type {
        /** ZIP file containing all the files and folders */
        ZIPSTREAM {
            @Override
            public FileTarget create(IParms processor, File outFileName) throws IOException {
                return new ZipStreamFileTarget(processor, outFileName);
            }
        },
        /** file-system directory */
        DIR {
            @Override
            public FileTarget create(IParms processor, File outFileName) throws IOException {
                return new DirFileTarget(processor, outFileName);
            }
        };

        /**
         * @return a file target handler of this type for the specified command processor
         *
         * @param processor		controlling command processor
         * @param outFileName	output file name, or NULL to use the default
         *
         * @throws IOException
         */
        public abstract FileTarget create(IParms processor, File outFileName) throws IOException;

    }

    /**
     * Construct a new file destination object.
     */
    public FileTarget() {
        // Save the base name and directory for default file name computation.
        var dform = new SimpleDateFormat("yyyy-MM-dd");
        this.baseName = "core" + dform.format(new Date());
        this.baseDir = new File(System.getProperty("user.dir"));
        // Denote no files have been copied yet.
        this.copyCount = 0;
    }

    /**
     * @return the default base directory
     */
    public File getBaseDir() {
        return this.baseDir;
    }

    /**
     * @return the default base file name
     */
    public String getBaseName() {
        return this.baseName;
    }

    /**
     * Start a new directory with the specified name.
     *
     * @param dirName	name for the new directory
     *
     * @throws IOException
     */
    public abstract void createDirectory(String dirName) throws IOException;

    /**
     * Copy a directory of files to the target.
     *
     * @param dirIn		input directory
     * @param dirOut	name to give to the output directory
     * @param files		array of names for the files to copy
     *
     * @throws IOException
     */
    public void dirCopy(File dirIn, String dirOut, String[] files) throws IOException {
        this.createDirectory(dirOut);
        this.copyIntoDir(dirIn, dirOut, files);
    }

    /**
    /**
     * Copy a directory of files to an existing target directory.
     *
     * @param dirIn		input directory
     * @param dirOut	name to give to the output directory
     * @param files		array of names for the files to copy
     *
     * @throws IOException
     */
    protected abstract void copyIntoDir(File dirIn, String dirOut, String[] files) throws IOException;

    /**
     * Denote another file has been copied.
     */
    protected void countFile() {
        this.copyCount++;
    }

    /**
     * @return the number of files copied
     */
    public int getCopyCount() {
        return this.copyCount;
    }

    /**
     * @return the output file/directory name
     */
    public abstract File getOutName();

    @Override
    public int hashCode() {
        File outName = this.getOutName();
        int result = ((outName == null) ? 0 : outName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof FileTarget)) {
            return false;
        }
        FileTarget other = (FileTarget) obj;
        File myName = this.getOutName();
        File otherName = other.getOutName();
        if (myName == null) {
            if (otherName != null) {
                return false;
            }
        } else if (!myName.equals(otherName)) {
            return false;
        }
        return true;
    }

}
