import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AverageComputationInMapper {
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		HashMap<String, ArrayList<Integer>> hashMap;
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			hashMap = new HashMap();
			super.setup(context);
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] strArray = line.split(" ");
			if(strArray.length > 1 && strArray[strArray.length - 1] != "-") {
				try {
					Integer comp_time = Integer.parseInt(strArray[strArray.length - 1]);
					if(hashMap.containsKey(strArray[0])) {
						ArrayList<Integer> list = hashMap.get(strArray[0]);
						list.add(comp_time);
						hashMap.put(strArray[0], list);
					}else {
						ArrayList<Integer> list = new ArrayList();
						list.add(comp_time);
						hashMap.put(strArray[0], list);
					}
					
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			for (HashMap.Entry<String, ArrayList<Integer>> entry : hashMap.entrySet()) {
				int sum = 0;
				int count = entry.getValue().size();
				for(Integer i : entry.getValue()) {
					sum += i;
				} 
				context.write(new Text(entry.getKey()), new IntWritable(sum / count));
			}
			super.cleanup(context);
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			for (IntWritable val : values) {
				sum += val.get();
				count += 1;
			}
			context.write(key, new IntWritable(sum / count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "averagecompute");
		job.setJarByClass(AverageComputationInMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}