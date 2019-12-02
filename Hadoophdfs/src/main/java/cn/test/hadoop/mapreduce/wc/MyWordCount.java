package cn.test.hadoop.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author QuLei
 */
public class MyWordCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        String[] otherArgs = parser.getRemainingArgs();
        //支持windows异构平台运行，采用自动获取的方式
        conf.set("mapreduce.app-submission.cross-platform", "true");
        //conf.set("mapreduce.framework.name","local");

        Job job = Job.getInstance(conf);
        //分布式集群不知道是那个类
        job.setJar("F:\\MSB\\BigData\\Bigdata\\Hadoophdfs\\target\\hadoop-hdfs-1.0-0.1.jar");
        job.setJarByClass(MyWordCount.class);

        // Specify various job-specific parameters
        job.setJobName("qulei");

        //job.setInputPath(new Path("in"));
        //job.setOutputPath(new Path("out"));
        Path infile = new Path(otherArgs[0]);
        TextInputFormat.addInputPath(job, infile);
        Path outfile = new Path(otherArgs[1]);
        if (outfile.getFileSystem(conf).exists(outfile)) {
            outfile.getFileSystem(conf).delete(outfile, true);
        }
        TextOutputFormat.setOutputPath(job, outfile);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);

        //job.setNumReduceTasks(2);
        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }
}

