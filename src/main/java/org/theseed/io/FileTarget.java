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
 * @author Bruce Parrello
 *
 */
public abstract class FileTarget implements AutoCloseable {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(FileTarget.class);
    /** name of the output file or directory */
    private File outName;
    /** copy counter */
    private int copyCount;

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
     *
     * @param outFileName	name of the output file or directory, or NULL to use the default
     */
    public FileTarget(File outFileName) {
        if (outFileName == null) {
            // Here we need the default file name.  It is put in the current directory and has the date built
            // in.
            File dir = new File(System.getProperty("user.dir"));
            var dform = new SimpleDateFormat("yyyy-MM-dd");
            String baseName = "core" + dform.format(new Date());
            this.outName = this.defaultFileName(dir, baseName);
        } else
            this.outName = outFileName;
        // Denote no files have been copied yet.
        this.copyCount = 0;
    }

    /**
     * Compute the default file name for this type.
     *
     * @param dir		target directory
     * @param baseName	base name of file
     *
     * @return the full file name
     */
    protected abstract File defaultFileName(File dir, String baseName);

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
    public File getOutName() {
        return this.outName;
    }

    @Override
    public int hashCode() {
        int result = ((this.outName == null) ? 0 : this.outName.hashCode());
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
        if (this.outName == null) {
            if (other.outName != null) {
                return false;
            }
        } else if (!this.outName.equals(other.outName)) {
            return false;
        }
        return true;
    }

}
