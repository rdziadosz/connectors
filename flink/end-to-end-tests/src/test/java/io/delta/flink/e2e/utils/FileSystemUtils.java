package io.delta.flink.e2e.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class FileSystemUtils {

    public static List<Path> listFiles(String basePath) throws IOException {
        List<Path> result = new ArrayList<>();

        Path path = new Path(basePath);
        FileSystem fileSystem = path.getFileSystem(new Configuration());
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(path, true);
        while (iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            result.add(file.getPath());
        }
        return result;
    }

    public static boolean locationExists(String basePath) throws IOException {
        Path path = new Path(basePath);
        FileSystem fileSystem = path.getFileSystem(new Configuration());
        return fileSystem.exists(path);
    }

    public static FileStatus getFileStatus(Path path) throws Exception {
        FileSystem fileSystem = path.getFileSystem(new Configuration());
        return fileSystem.getFileStatus(path);
    }

    public static FileStatus getFileStatus(String basePath) throws Exception {
        return getFileStatus(new Path(basePath));
    }

}
