package cn.test.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

public class TestHDFS {
    public Configuration conf;
    public FileSystem fs;

    @Before
    public void conn() throws Exception {
        conf = new Configuration(true);
        //采用环境变量的方式创建
        /*
        fs = FileSystem.get(conf);*/

        //
        fs = FileSystem.get(URI.create("hdfs://mycluster"), conf, "god");

    }

    @Test
    public void mkdir() throws IOException {
        Path dir = new Path("/qulei01");
        if (fs.exists(dir)) {
            fs.delete(dir, true);
        }
        fs.mkdirs(dir);
    }

    @Test
    public void upload() throws Exception {
        BufferedInputStream input = new BufferedInputStream(new FileInputStream(new File("./data/hello.txt")));
        Path outfile = new Path("/qulei/out.txt");
        FSDataOutputStream output = fs.create(outfile);
        IOUtils.copyBytes(input, output, conf, true);

    }

    @Test
    public void blocks() throws IOException {
        Path file =new Path("/user/god/data.txt");
        FileStatus fss = fs.getFileStatus(file);
        BlockLocation[] locations = fs.getFileBlockLocations(fss, 0, fss.getLen());
        for (BlockLocation b:locations) {
            System.out.println(b);
        }
    }

    @After
    public void close() throws Exception {
        fs.close();
    }
}
