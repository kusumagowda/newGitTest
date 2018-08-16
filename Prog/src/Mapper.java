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


public class Mapper {
		public static class RedisOutPutMapper extends
		Mapper<Object, Text, Text, Text> {
			private Text outkey = new Text();
			private Text outvalue = new Text();
			public void map(Object key, Text value, Context context)
			   throws IOException, InterruptedException {
				Map<String, String> parsed= MRDPUtls.transformaXmlToMap(value.toString());
				String userId=parsed.get("Id");
				String reputation= parsed.get("Reputation");
			   outkey.set(userId);
			    outvalue.set(reputation);
			    context.write(outkey, outvalue);
			    
		}
		}
		}
