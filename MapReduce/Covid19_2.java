
import java.io.IOException;
import java.util.*;
import java.text.SimpleDateFormat;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Covid19_2 {
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		Text textKey = new Text();
		LongWritable val = new LongWritable();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		// The 4 types declared here should match the types that was declared on the top
		//value is first line for that node. Text is somewhat like string.
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			Configuration conf = context.getConfiguration();
			String line = value.toString();
			String[] field = line.split(",");
			String date = field[0];
			Date d1 = null;
			Date d2 = null;
			Date d = null;
			try
			{
				d1 = format.parse(conf.get("startdate"));
				d2 = format.parse(conf.get("enddate"));
				d = format.parse(date);
				if(field.length == 4)
				{

					if(d1.compareTo(d) * d.compareTo(d2) >=0 )
					{
						int deaths = Integer.parseInt(field[3]);
						textKey.set(field[1]);
						val.set(deaths);
						context.write(textKey, val);
					}
				}

			} catch (Exception e){
				e.printStackTrace();
			}
			
		}	
	}


	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	// The input types of reduce should match the output type of map
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		LongWritable longvalue = new LongWritable();
		// Notice the that 2nd argument: type of the input value is an Iterable collection of objects 
		//  with the same type declared above/as the type of output value from map
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum_death = 0;
			for (LongWritable val: values) {
				sum_death += val.get();
			}
			// This write to the final output
			longvalue.set(sum_death);
			context.write(key, longvalue);
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		long startTime = System.nanoTime();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		try
		{
			Date startdate = format.parse(args[1]);
			Date enddate = format.parse(args[2]);
			Date rangestart = format.parse("2019-12-31");
			Date rangeend = format.parse("2020-04-08");
			if(startdate.compareTo(enddate)>0) 
				throw new Exception("End date less than start date");

			if(rangestart.compareTo(startdate)>0 ||
				enddate.compareTo(rangeend)>0) 
				throw new Exception("Date not in range");

			Configuration conf = new Configuration();
			conf.set("startdate", args[1]);
			conf.set("enddate", args[2]);
			Job myjob = Job.getInstance(conf, "my word count test");
			myjob.setJarByClass(Covid19_2.class);
			myjob.setMapperClass(MyMapper.class);
			myjob.setReducerClass(MyReducer.class);
			myjob.setOutputKeyClass(Text.class);
			myjob.setOutputValueClass(LongWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
			FileInputFormat.addInputPath(myjob, new Path(args[0]));
			FileOutputFormat.setOutputPath(myjob,  new Path(args[3]));
			myjob.waitForCompletion(true);
			long endTime = System.nanoTime();
			long timeElapsed = endTime - startTime;
			System.out.println("Execution time in seconds : " + timeElapsed / 1000000000);
			System.exit(myjob.waitForCompletion(true) ? 0 : 1);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		
	}
}
