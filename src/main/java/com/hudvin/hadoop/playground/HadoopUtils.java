package com.hudvin.hadoop.playground;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by kontiki on 10.05.16.
 */
public class HadoopUtils {

    public static void removeOutputDir(Path outputDir, Configuration conf) throws IOException {
        FileSystem fs = outputDir.getFileSystem(conf);
        if (outputDir.toString().contains("/tmp") && fs.exists(outputDir)){
            fs.delete(outputDir, true);
        }
    }

}
