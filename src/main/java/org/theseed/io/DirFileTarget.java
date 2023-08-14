/**
 *
 */
package org.theseed.io;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

/**
 * This represents a file target stored directly in the file system, rather than in an archive.
 *
 * @author Bruce Parrello
 *
 */
public class DirFileTarget extends FileTarget {

    public DirFileTarget(IParms processor, File outFileName) throws IOException {
        super(outFileName);
        // If the output file name is NULL, this gets us the default name; otherwise, it
        // returns the caller-supplied name.
        File outFile = this.getOutName();
        if (! outFile.isDirectory()) {
            log.info("Creating output directory {}.", outFile);
            FileUtils.forceMkdir(outFile);
        } else if (processor.shouldErase()) {
            log.info("Erasing output directory {}.", outFile);
            FileUtils.cleanDirectory(outFile);
        }
    }

    @Override
    protected File defaultFileName(File dir, String baseName) {
        return new File(dir, baseName);
    }

    @Override
    public void createDirectory(String dirName) throws IOException {
        File dir = this.getDir(dirName);
        if (! dir.isDirectory())
            FileUtils.forceMkdir(dir);
    }

    /**
     * @return the file name for the specified directory
     *
     * @param dirName	directory name
     */
    private File getDir(String dirName) {
        return new File(this.getOutName(), dirName);
    }

    @Override
    protected void copyIntoDir(File dirIn, String dirOut, String[] files) throws IOException {
        // Get the output directory.
        File dir = this.getDir(dirOut);
        // Loop through the files, copying.
        for (String file : files) {
            // Verify the source file exists.
            File sourceFile = new File(dirIn, file);
            if (sourceFile.exists()) {
                // Copy it to the target directory.
                FileUtils.copyFile(sourceFile, new File(dir, file));
                this.countFile();
            }
        }
    }

    @Override
    public void close() throws Exception {
    }


}
