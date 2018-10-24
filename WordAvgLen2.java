//This is the first version utilizes a combiner
//Writer Mengqi Zhang
//Last modified 26/8/2017

package comp9313.ass1;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;


//Second version of utilizing the in-mapper combing
public class WordAvgLen2 {//Mapper is going to read text
	public static class AvgLen2Mapper
    extends Mapper<Object, Text, Text, MapWritable>{

 private MapWritable Container = new MapWritable();
 private Text Key = new Text();
 Map<String, Integer> Length_sum = new HashMap<String, Integer>();
 Map<String, Integer> Number_count = new HashMap<String, Integer>();
 public void map(Object key, Text value, Context Value
                 ) throws IOException, InterruptedException {
     //utilize tokenizer to divide the text into different part as the specification required
	 StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
	 while (itr.hasMoreTokens()) {
	    String a = itr.nextToken().toLowerCase();
		String b = String.valueOf(a.charAt(0));//try to get the first char
		if (b.charAt(0)>='a' && b.charAt(0)<='z'){
//the idea is use the first char as a key and to utilize two hashmaps to store the words' length and number
           if (Length_sum.containsKey(b)){
        	int sum =(Integer)a.length() + (Integer) Length_sum.get(b); 
        	Length_sum.put(b, sum);
        }

           else{
        	   Length_sum.put(b,(Integer)a.length());}
           if (Number_count.containsKey(b)){
        	int count =1 + (Integer) Number_count.get(b); 
        	Number_count.put(b, count);
        }
           else{
        	Number_count.put(b,1);}
		}
   }
	 
 }
//utilize in-mapper combining to calculate the sum of length and number
 protected void cleanup(Context Value) throws IOException, InterruptedException {
	 Iterator<?> iter = Length_sum.entrySet().iterator();
	 while(iter.hasNext()){
			Map.Entry entry = (Map.Entry) iter.next();
			String k = (String) entry.getKey();
         Integer Length_sum_temp = (Integer) Length_sum.get(k);
         Integer Number_count_temp = (Integer) Number_count.get(k);
         Container.put(new IntWritable(0),new IntWritable(Length_sum_temp));
         Container.put(new IntWritable(1),new IntWritable(Number_count_temp));
 		Key.set(k);
 		Value.write(Key,Container);
		}
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
        job.setOutputFormatClass(TextOutputFormat.class);  
        job.setJarByClass(WordAvgLen1.class);
	    job.setMapperClass(AvgLen2Mapper.class);
	    job.setReducerClass(SumReducer.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	
	
}
