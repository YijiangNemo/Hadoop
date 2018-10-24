//This is the first version utilizes a combiner
//Writer Mengqi Zhang
//Last modified 26/8/2017

package comp9313.ass1;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WordAvgLen1 {//Mapper is going to read text
	public static class AvgLen1Mapper
    extends Mapper<Object, Text, Text, MapWritable>{

 private MapWritable container = new MapWritable();
 private Text Key = new Text();
 public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
     //utilize tokenizer to divide the text into different part as the specification required
	 StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
	 while (itr.hasMoreTokens()) {
	    String a = itr.nextToken().toLowerCase();
		String b = String.valueOf(a.charAt(0));     //try to get the first char
		if (b.charAt(0)>='a' && b.charAt(0)<='z'){
		Key.set(b);
        //utilize the first char as a key and utilize mapwritable to store words' length and number
		container.put(new IntWritable(0),new IntWritable(a.length()));
		container.put(new IntWritable(1),new IntWritable(1));
		context.write(Key,container);
		}
   }
 }
	}
	public static class combiner  //utilize combiner to calculate the sum of length and number
	extends Reducer<Text, MapWritable, Text, MapWritable> {
	    private MapWritable Combinerresult = new MapWritable();
	    public void reduce(Text key, Iterable<MapWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
        //initialize the sum of length and number
	      int Length_sum = 0;
	      int Number_sum = 0;
	      for (MapWritable v : values) {
	    	int x = ((IntWritable) v.get(new IntWritable(0))).get();
	    	int y = ((IntWritable) v.get(new IntWritable(1))).get();
	    	Length_sum += x;
	    	Number_sum += y;
	      }
	      
	      Combinerresult.put(new IntWritable(0),new IntWritable(Length_sum));
	      Combinerresult.put(new IntWritable(1),new IntWritable(Number_sum));
	      context.write(key, Combinerresult);
	    }
	  }

public static class SumReducer
     extends Reducer<Text,MapWritable,Text,DoubleWritable> {
  private DoubleWritable Output = new DoubleWritable();

  public void reduce(Text key, Iterable<MapWritable> values,
                     Context Value
                     ) throws IOException, InterruptedException {
	double sum = 0;
	double count = 0;

    for (MapWritable v : values) {
      double x = ((IntWritable) v.get(new IntWritable(0))).get();
      double y = ((IntWritable) v.get(new IntWritable(1))).get();
      sum += x;
      count += y;
     
    }
    Output.set(sum/count);
    Value.write(key, Output);
  }
}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "WordAvgLen");
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(MapWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class); 
	    job.setJarByClass(WordAvgLen1.class);
	    job.setMapperClass(AvgLen1Mapper.class);
	    job.setCombinerClass(combiner.class);
	    job.setReducerClass(SumReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);  
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	
	
}
