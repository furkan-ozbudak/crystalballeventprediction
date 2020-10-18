package cbStripes;

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

public class CbStripes {

	public static class Map extends Mapper<Object, Text, Text, MapWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			final IntWritable ONE = new IntWritable(1);
			String[] s = value.toString().split("\\s+");
			int n = s.length;

			for (int i = 0; i < n - 1; i++) {
				MapWritable m = new MapWritable();

				for (int j = i + 1; j < n; j++) {
					if (s[i].equals(s[j])) {
						break;
					} else {
						if (m.containsKey(new Text(s[j]))) {
							m.put(new Text(s[j]),
									new IntWritable(((IntWritable) m
											.get(new Text(s[j]))).get() + 1));
						} else {
							m.put(new Text(s[j]), ONE);
						}
					}
				}
				context.write(new Text(s[i]), m);
			}
		}
	}

	public static class Reduce extends Reducer<Text, MapWritable, Text, Text> {

		public void reduce(Text key, Iterable<MapWritable> values,
				Context context) throws IOException, InterruptedException {

			double grandTotal = 0;

			MapWritable fm = new MapWritable();

			for (MapWritable m : values) {
				for (Entry e : m.entrySet()) {
					if (fm.containsKey(e.getKey())) {
						fm.put((Text) e.getKey(),
								new IntWritable(((IntWritable) e.getValue())
										.get()
										+ ((IntWritable) fm.get(e.getKey()))
												.get()));
						grandTotal += ((IntWritable) e.getValue()).get();
					} else {
						fm.put((Text) e.getKey(), new IntWritable(
								((IntWritable) e.getValue()).get()));
						grandTotal += ((IntWritable) e.getValue()).get();
					}

				}
			}

			String output = key.toString() + "\n";

			for (Entry e : fm.entrySet()) {
				double c = (double) ((IntWritable) e.getValue()).get();
				double frequency = c / grandTotal;
				output += e.getKey().toString() + " , "
						+ String.valueOf(frequency) + "\n";
			}

			context.write(new Text(""), new Text(output));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "cbstripes");
		job.setJarByClass(CbStripes.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
