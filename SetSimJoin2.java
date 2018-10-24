package comp9313.ass4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SetSimJoin2 {	
	// ------------------Stage_1: calculate the similarity-----------------
	/**
	 * This is the Mapper for SetSimJoin.
	 * 
	 */
	public static class SSJMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// split the input line into string array.
			String[] element = value.toString().split(" ");
			// get the similarity threshold
			Configuration conf = context.getConfiguration();
			double similarity = conf.getDouble("similarity", 0.0);
			// edit the upper bound of the number of emit key-value pairs to
			// ensure the result is precise.
			int upperbound = (int) Math.ceil((element.length - 1) * (1 - similarity));
			for (int i = 1; i <= upperbound; i++) {
				context.write(new Text(element[i]), value);
			}
		}
	}

	/**
	 * This is the Reducer for SetSimJoin.
	 *
	 */
	public static class SSJReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<String> list = new ArrayList<String>();
			// put all these candidates' value into a list
			for (Text val : values) {
				list.add(val.toString());
			}

			// calculate the similarity of all these candidates
			for (int i = 0; i < list.size(); i++) {
				String[] str1 = list.get(i).split(" ");
				int length = str1.length;
				// get the record ID for record_1
				String record = str1[0];
				// create a hash set put all element of record_1 into the set
				HashSet<String> set = new HashSet<String>();
				for (int k = 1; k < length; k++) {
					set.add(str1[k]);
				}
				// for each candidate record
				for (int j = i + 1; j < list.size(); j++) {
					double same = 0.0;
					str1 = list.get(j).split(" ");
					// go through all element in record_2, count the same record.
					for (int k = 1; k < str1.length; k++) {
						if (set.contains(str1[k])) {
							same++;
						}
					}
					// calculate the similarity.
					double Sim = same / (length - 1 + str1.length - 1 - same);
					// get the similarity threshold
					Configuration conf = context.getConfiguration();
					double similarity = conf.getDouble("similarity", 0.0);
					// if similarity is larger than the threshold, emit it to reducer
					if (Sim >= similarity) {
						// sort the pair, put the smaller one on front
						if (Integer.parseInt(str1[0]) < Integer.parseInt(record))
							context.write(new Text(str1[0] + "," + record), new DoubleWritable(Sim));
						else
							context.write(new Text(record + "," + str1[0]), new DoubleWritable(Sim));
					}
				}
			}
		}
	}

	// ------------------Stage2: Reduce the duplicate.-----------------------
	/**
	 * This is the Mapper for remove the duplicate.
	 */
	public static class EndMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] result = value.toString().split("\t");
			// get the pair value, put the into Pair structure to convenient the sort process.
			context.write(new Text(result[0]), new Text(result[1]));
		}
	}

	public static class EndCombiner extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
			// get the value information and output it with the required format
			Iterator<Text> vals = values.iterator();
			context.write(key, new Text(vals.next().toString()));
		}
	}

	
	/**
	 * This is the Reducer for remove the duplicate.
	 */
	public static class EndReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// get the value information and output it with the required format
			Iterator<Text> vals = values.iterator();
			context.write(new Text("(" + key.toString() + ")"), new Text(vals.next().toString()));
		}
	}


	/**
	 * Partitioner for remove duplicate
	 * 
	 * @author brook
	 */
    public static class SSJPartitioner extends Partitioner<Text, Text>{

		@Override
		public int getPartition(Text key, Text value, int numOfReducer) {
			return Math.abs(key.toString().split(",")[0].hashCode()) % numOfReducer;
		}
    }
    
	/**
	 * Customize Sort comparator class.
	 *
	 */
	public static class SSJSortComparator extends WritableComparator {

		protected SSJSortComparator() {
			super(Text.class, true);
		}

		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable sp1, WritableComparable sp2) {
			// get two argument of sp1 and sp2
			String[] pair1 = ((Text) sp1).toString().split(",");
			String[] pair2 = ((Text) sp2).toString().split(",");
			// sorted by key in ascending order
			if(pair1[0].hashCode() > pair2[0].hashCode())
				return 1;
			else if(pair1[0].hashCode() < pair2[0].hashCode())
				return -1;
			else if(pair1[1].hashCode() > pair2[1].hashCode())
				return 1;
			else if(pair1[1].hashCode() < pair2[1].hashCode())
				return -1;
			else
				return 0;
		}
	}

	/**
	 * This is the driver for Set Similarity Join.
	 */
	public static void main(String[] args) throws Exception {
		// Stage_1: Calculate the similarity
		// get the the number of reducer
		int NumOfReducer = Integer.parseInt(args[3]);
		// set the configure and job
		Configuration conf = new Configuration();
		// read the threshold for similarity from the input arguments.
		conf.setDouble("similarity", Double.parseDouble(args[2]));
		Job job = Job.getInstance(conf, "Set Similarity Join2");
		job.setJarByClass(SetSimJoin2.class);
		// Set the corresponding class for map and reduce
		job.setMapperClass(SSJMapper.class);
		job.setReducerClass(SSJReducer.class);
		// Set the Data Type for mapper's output.
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// Set the Data Type for reducer's output.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		// Set the number of reducer.
		job.setNumReduceTasks(NumOfReducer);
		// Set the input path and output path.
		FileInputFormat.addInputPath(job, new Path(args[0]));
		String output = args[1] + System.nanoTime();
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);

		// Stage_2: Remove the duplicate
		// set the configure and job
		conf = new Configuration();
		job = Job.getInstance(conf, "Set Similarity Join2");
		job.setJarByClass(SetSimJoin2.class);
		// Set the corresponding class for map, combine and reduce
		job.setMapperClass(EndMapper.class);
		job.setCombinerClass(EndCombiner.class);
		job.setReducerClass(EndReducer.class);
		// Set the Data Type for mapper's output.
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// Set the Data Type for reducer's output.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// set the partition class
		job.setPartitionerClass(SSJPartitioner.class);
		// Set sort comparator class. 
		job.setSortComparatorClass(SSJSortComparator.class);
		// Set the number of reducer.
		job.setNumReduceTasks(NumOfReducer);
		// Set the input path and output path.
		FileInputFormat.addInputPath(job, new Path(output));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
