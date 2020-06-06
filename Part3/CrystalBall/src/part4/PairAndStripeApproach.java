package part4;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import part4.Pair;

public class PairAndStripeApproach {

	public static class Map extends Mapper<LongWritable, Text, Pair, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
				
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] strArray = line.split(" ");
			for(int i = 0; i < strArray.length; i++) {
				for(int j = i + 1; j < strArray.length; j++) {
					if(!strArray[i].equals(strArray[j])) context.write(new Pair(strArray[i], strArray[j]), one);
					else break;
				}
			}
		}
	}

//	public static class Reduce extends Reducer<Pair, MapWritable, Text, MapWritable> {
//
//		public void reduce(Pair key, Iterable<Pair> values, Context context)
//				throws IOException, InterruptedException {
//			
//			for (MapWritable mapW : values) {
//				for (Entry<Writable, Writable> entry : mapW.entrySet()) {
//					Text k = (Text)entry.getKey();
//					IntWritable countInSum = (IntWritable)sum.get(k); 
//					IntWritable nCount = (IntWritable) entry.getValue();
//					if (sum.containsKey(k)) 
//						sum.put(k, new IntWritable(countInSum.get() + nCount.get()));
//					else
//						sum.put(k, entry.getValue());
//				}
//			}
//			context.write(key, sum);
//		}
//	}
	
	public static class Reduce extends Reducer<Pair, IntWritable, Pair, IntWritable> {

		MapWritable mapW; 
		
		@Override
		protected void setup(Reducer<Pair, IntWritable, Pair, IntWritable>.Context context)
				throws IOException, InterruptedException {
			mapW = new MapWritable();
			super.setup(context);
		}

		public void reduce(Pair pair, Iterable<IntWritable> values, Context context)		
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values)
				sum += val.get();
			if(!mapW.containsKey(pair.key1)) {
				mapW.put(key1, )
			}
		}
		
		@Override
		protected void cleanup(Reducer<Pair, IntWritable, Pair, IntWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(pair, new IntWritable(sum));
			super.cleanup(context);
		}
		
		
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "crystal");
		job.setJarByClass(PairAndStripeApproach.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
