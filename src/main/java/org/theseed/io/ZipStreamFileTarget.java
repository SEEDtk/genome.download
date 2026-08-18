/**
 *
 */
package org.theseed.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * This is a file target implemented as a zip file.
 *
 * @author Bruce Parrello
 *
 */
public class ZipStreamFileTarget extends FileTarget {

    // FIELDS
    /** read buffer */
    private final byte[] buffer;
    /** proposed buffer size */
    private static final int BUFFER_SIZE = 4096;
    /** ouput file name */
    private final File outName;
    /** zip file output stream */
    private final ZipOutputStream zipStream;
    /** target file output stream */
    private final OutputStream outStream;

    public ZipStreamFileTarget(FileTarget.IParms processor, File outFileName) throws IOException {
        super();
        // Allocate the read buffer.
        this.buffer = new byte[BUFFER_SIZE];
        // Compute the output file name.  If the user did not specify one, we use the default.
        if (outFileName == null)
            this.outName = new File(this.getBaseDir(), this.getBaseName() + ".zip");
        else
            this.outName = outFileName;
        // Open the zip output stream.
        this.outStream = new FileOutputStream(this.outName);
        this.zipStream = new ZipOutputStream(this.outStream);
    }

    @Override
    public void createDirectory(String dirName) throws IOException {
        this.zipStream.putNextEntry(new ZipEntry(dirName));
        this.zipStream.closeEntry();
    }

    @Override
    protected void copyIntoDir(File dirIn, String dirOut, String[] files) throws IOException {
        for (String fileName : files) {
            File oldFile = new File(dirIn, fileName);
            if (oldFile.exists()) {
                // This automatically creates the output directory.
                ZipEntry fileEntry = new ZipEntry(dirOut + fileName);
                this.zipStream.putNextEntry(fileEntry);
                try (FileInputStream oldStream = new FileInputStream(oldFile)) {
                    for (int len = oldStream.read(buffer); len >= 0; len = oldStream.read(buffer))
                        this.zipStream.write(this.buffer, 0, len);
                }
                this.zipStream.closeEntry();
                this.countFile();
            }
        }
    }

    @Override
    public void close() {
        try {
            if (this.zipStream != null)
                this.zipStream.close();
            if (this.outStream != null)
                this.outStream.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public File getOutName() {
        return this.outName;
    }

}
