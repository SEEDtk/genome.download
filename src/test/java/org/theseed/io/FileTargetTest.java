/**
 *
 */
package org.theseed.io;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class FileTargetTest implements FileTarget.IParms {

    private boolean eraseFlag;

    @Test
    public void testDirTarget() throws IOException {
        this.eraseFlag = false;
        File parentDir = new File("data");
        File tempDir = new File("data", "temp");
        File testDir = new File("data", "test1");
        String[] files = new String[] { "map.predictions.tbl", "mapReport.tbl", "mapSreport.tbl", "thr24.predictions.tbl" };
        FileTarget dirTarget = FileTarget.Type.DIR.create(this, parentDir);
        assertThat(dirTarget.getOutName().exists(), equalTo(true));
        String outDir = "temp/";
        dirTarget.dirCopy(testDir, outDir, files);
        for (String file : files) {
            File file1 = new File(testDir, file);
            File file2 = new File(tempDir, file);
            assertThat(file, FileUtils.contentEquals(file1, file2), equalTo(true));
        }
        File testFile = new File(tempDir, "map.production.tbl");
        assertThat(testFile.exists(), equalTo(false));
        assertThat(dirTarget.getCopyCount(), equalTo(4));
    }

    @Override
    public boolean shouldErase() {
        return this.eraseFlag;
    }

}
