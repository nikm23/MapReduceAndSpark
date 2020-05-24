
import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Covid19_1 {
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, Text> {
		Text textKey = new Text();
		Text textval = new Text();
		FloatWritable floatWritable = new FloatWritable();
		// The 4 types declared here should match the types that was declared on the top
		//value is first line for that node. Text is somewhat like string.
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			boolean flag = Boolean.parseBoolean(conf.get("world"));
			String line = value.toString();
			String[] field = line.split(",");
			if(field.length == 4)
			{
				if(flag || (field[1]!="World" && field[1]!="International"))
				{
					String date = field[0];
					if(date.contains("2020"))
					{
						int cases = Integer.parseInt(field[2]);
						int deaths = Integer.parseInt(field[3]);
						String val = String.valueOf(cases)+","+String.valueOf(deaths);
						textKey.set(field[1]);
						textval.set(val);
						context.write(textKey, textval);
					}
				}
			}
		}
		
	}


	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	// The input types of reduce should match the output type of map
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		Text textval = new Text();
		// Notice the that 2nd argument: type of the input value is an Iterable collection of objects 
		//  with the same type declared above/as the type of output value from map
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			long sum_case = 0;
			long sum_death = 0;
			for (Text val: values) {
				String line = val.toString();
				String[] field = line.split(",");
				sum_case += Integer.parseInt(field[0]);
				sum_death += Integer.parseInt(field[1]);
			}
			String v = String.valueOf(sum_case)+"\t"+String.valueOf(sum_death);
			// This write to the final output
			textval.set(v);
			context.write(key, textval);
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		Configuration conf = new Configuration();
		conf.set("world", args[1]);
		Job myjob = Job.getInstance(conf, "my word count test");
		myjob.setJarByClass(Covid19_1.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(Text.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[2]));
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
}
