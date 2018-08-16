import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.security.InvalidParameterException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Driver {
public static void main(String[] args) throws Exception {
	Configuration conf=new Configuration();
	Path inputPath= new Path(args[0]);
	String hosts= args[1];
	String hashname= args[2];
	Job job = new Job(conf, "Redis Output");
	job.setJarByClass(RedisOutputDriver.class);
	job.setMapperClass(RedisOutputMapper.class);
	job.setNumReduceTasks(0);
	job.setInputFormatClass(TextInputFormat.class);
	TextInputFormat.setInputPaths(job, inputPath);
	job.setOutputFormatClass(RedisHashOutputFormat.class);
	RedisHashOutputFormat.setRedisHashKey(job, hosts);
	RedisHashOutputFormat.setRedisHashKey(job, hashname);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	int code= job.waitForCompletion(true)? 0:2;
		System.exit(code);
}
}