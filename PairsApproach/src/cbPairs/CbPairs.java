package cbPairs;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CbPairs {

	public static class Map extends Mapper<Object, Text, Pair, IntWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			final IntWritable ONE = new IntWritable(1);
			String[] s = value.toString().split("\\s+");
			int n = s.length;

			for (int i = 0; i < n - 1; i++) {
				for (int j = i + 1; j < n; j++) {
					if (s[i].equals(s[j])) {
						break;
					} else {
						context.write(new Pair(new Text(s[i]), new Text("*")),
								ONE);
						context.write(new Pair(new Text(s[i]), new Text(s[j])),
								ONE);
					}
				}
			}
		}
	}

	public class CustomPartitioner extends Partitioner<Pair, Text> {
		public int getPartition(Pair key, Text value, int numReduceTasks) {
			return Math.abs(key.getItem1().hashCode()) % numReduceTasks;
		}
	}

	public static class Reduce extends
			Reducer<Pair, IntWritable, Pair, DoubleWritable> {
		private int grandTotal = 0;

		public void reduce(Pair key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int total = 0;
			for (IntWritable one : values) {
				total++;
			}

			if (key.getItem2().toString().equals("*")) {
				grandTotal = total;
			} else {
				context.write(key, new DoubleWritable((double) total / grandTotal ));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "cbpairs");
		job.setJarByClass(CbPairs.class);

		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setPartitionerClass(CustomPartitioner.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
