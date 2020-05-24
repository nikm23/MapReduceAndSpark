
import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.URI;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Covid19_3 {



	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		Text textKey = new Text();
		LongWritable val = new LongWritable();
		// The 4 types declared here should match the types that was declared on the top
		//value is first line for that node. Text is somewhat like string.


		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			Configuration conf = context.getConfiguration();
			String line = value.toString();
			String[] field = line.split(",");
			if (value.toString().contains("date"))
				return;
			if(field.length == 4)
			{
				int cases = Integer.parseInt(field[2]);
				textKey.set(field[1]);
				val.set(cases);
				context.write(textKey, val);
			}
		}
		
	}


	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	// The input types of reduce should match the output type of map
	public static class MyReducer extends Reducer<Text, LongWritable, Text, FloatWritable> {
		private FloatWritable avgcase = new FloatWritable();
		// Notice the that 2nd argument: type of the input value is an Iterable collection of objects 
		//  with the same type declared above/as the type of output value from map
		Map<String, Integer> pop = new HashMap<String, Integer>();
		public void setup(Context context) 
		{
         // Get the cached archives/files
			try{
				URI[] files = context.getCacheFiles();
            	if(files != null && files.length>0){
            		String line = "";
            		FileSystem fs = FileSystem.get(context.getConfiguration());
            		Path getFilePath = new Path(files[0].toString());
            		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
            		reader.readLine();
            		while((line = reader.readLine())!=null){
            			String[] words = line.split(",");
            			if(words.length==5){
            				pop.put(words[0], Integer.parseInt(words[4]));
            			}
            		}
            	}
            }
        	catch(Exception e){
            	e.printStackTrace();
        	}
        }


		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			String country = key.toString();
			for (LongWritable val: values) {
				sum += val.get();
			}
			try{
				float population = pop.get(country);
				float avg = (sum/population)*1000000;
				avgcase.set(avg);
				context.write(key, avgcase);
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		long startTime = System.nanoTime();
		Configuration conf = new Configuration();
		Job myjob = Job.getInstance(conf);
		myjob.addCacheFile(new Path("hdfs://sandbox-hdp.hortonworks.com:8020"+args[1]).toUri());
		myjob.setJarByClass(Covid19_3.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[2]));
		myjob.waitForCompletion(true);
		long endTime = System.nanoTime();
		long timeElapsed = endTime - startTime;
		System.out.println("Execution time in seconds : " + timeElapsed / 1000000000);
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
}
