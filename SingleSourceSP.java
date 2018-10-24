package comp9313.ass2;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SingleSourceSP {
    
    public static String OUT = "output";
    public static String IN = "input";
    public static String Source_Point = "S_P";
    
    // create a counter.
    public static enum MODIFY_COUNTER {
        UPDATE; // used to record the update time
    };
    
    /*
     * Mapper class for preprocessing the input file.
     */
    public static class FirstMapper extends Mapper<Object, Text, Text, Text> {
        private Text id = new Text();
        private Text source_Node = new Text();
        private Text dst_Node = new Text();
        private Text dis = new Text();
        @Override
        public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            //give the value to the text parameters
            //and change the position of dst_Node and source_Node to calculate the dis from aim node to query node
            id.set(tokenizer.nextToken());
            source_Node.set(tokenizer.nextToken());
            dst_Node.set(tokenizer.nextToken());
            dis.set(tokenizer.nextToken());
            
            context.write(source_Node, new Text(dst_Node + ":" + dis));
        }
    }
    
    
    /*
     * Reducer class for preprocessing the input file.
     */
    public static class FirstReducer extends Reducer<Text, Text, Text, Text> {
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            
            StringBuilder tem_sb = new StringBuilder();
            for (Text tmp_v : values) {
                
                if (tem_sb.length() != 0) {
                    tem_sb.append(";");
                }
                tem_sb.append(tmp_v.toString());
            }
            
            double inf = Double.MAX_VALUE;
            
            if (key.toString().equals(Source_Point)) {
                inf = 0;
            }
            // Transform to the input format.
            // Input Format: SourceNode Distance (ToNode: dis; ToNode: dis; ...)
            context.write(key, new Text(inf + "\t" + tem_sb.toString()));
        }
    }
    
    /*
     * Mapper for calculate the shortest dis.
     */
    public static class SRMapper extends Mapper<Object, Text, Text, Text> {
        
        @Override
        public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {  
            String line = value.toString();
            String[] line_splited = line.split("\t");
            
            double dis = Double.parseDouble(line_splited[1]);
            
            if(line_splited.length > 3){
                
                String[] second_split = line_splited[3].split(";");
                for (int i = 0; i < second_split.length; i++) {
                    
                    String[] third_split = second_split[i].split(":");
                    
                    double add_dis_value = Double.parseDouble(third_split[1]) + dis;
                    
                    
                    context.write(new Text(third_split[0]), new Text(add_dis_value+"#"+ line_splited[2]));}
                
                context.write(new Text(line_splited[0]), new Text(line_splited[1]+"#"+line_splited[2] +"#"+line_splited[3]));
            }
            else if(line_splited.length > 2)
            {String[] second_split = line_splited[2].split(";");
                for (int i = 0; i < second_split.length; i++) {
                    
                    String[] third_split = second_split[i].split(":");
                    
                    double add_dis_value = Double.parseDouble(third_split[1]) + dis;
                    context.write(new Text(third_split[0]), new Text(add_dis_value +"#"+Source_Point));
                }
                
                context.write(new Text(line_splited[0]), new Text(line_splited[1]+"#"+Source_Point +"#"+line_splited[2]));
            }
            
        }
    }
    
    
    
    /*
     * Reducer for calculate the shortest dis.
     */
    public static class SRReducer extends Reducer<Text, Text, Text, Text> {
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            
            String adj_nodes = "";
            String record_path = "";
            String old_path = "";
            // create a list to store the dis value
            LinkedList<Double> list = new LinkedList<Double>();
            LinkedList<String> list_record_path = new LinkedList<String>();
            // initialize the min with maximum value of double
            double min = Double.MAX_VALUE;
            for (Text tmp_v : values) {
                // split the tmp_v by the signal.
                String tmp_v1 = tmp_v.toString();
                String[] line_splited = tmp_v1.split("#");
                
                if (line_splited.length > 2) {
                    
                    min = Double.parseDouble(line_splited[0]);
                    old_path = line_splited[1];
                    adj_nodes = line_splited[2];
                } else {
                    
                    double dis = Double.parseDouble(line_splited[0]);
                    
                    record_path = line_splited[1];
                    list.add(dis);
                    list_record_path.add(record_path);
                    
                }
                
            }
            
            boolean x = false;
            for (double dis : list) {
                // check whether there are some value smaller than last iteration(Previous dis).
                if (min > dis) {
                    min = dis;
                    // count the update.
                    context.getCounter(MODIFY_COUNTER.UPDATE).increment(1);
                    x = true;
                }
            }
            if(adj_nodes==""){adj_nodes = key+":"+Double.MAX_VALUE;}
            if(old_path==""){old_path = Source_Point;}
            if(x == true){
                if(list.contains(min)){
                    record_path = list_record_path.get(list.indexOf(min));
                }
                context.write(key, new Text(min + "\t" + record_path+"->"+ key+ "\t" + adj_nodes  ));}
            else{context.write(key, new Text(min + "\t" + old_path+ "\t" + adj_nodes  ));}
        }
    }
    
    /*
     * Mapper class for process the result to satisfied the requirement.
     */
    public static class EndMapper extends Mapper<Object, Text, IntWritable, Text> {
        
        @Override
        public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
            // INPUT FORMAT: SourceNode: 0 Distance: 1 Adjacency List 2:3.0
            String line = value.toString();
            // splits by "\t"
            
            String[] line_splited = line.split("\t");
            
            String result = line_splited[1].toString()+":"+line_splited[2].toString();
            
            // passing the information to reducer. (using MapReduce's default sort to sort the node)
            context.write(new IntWritable(Integer.parseInt(line_splited[0])), new Text(result));
        }
    }
    
    /*
     * Reducer class for process the result to satisfied the requirement.
     */
    public static class EndReducer extends Reducer<IntWritable, Text, Text, Text> {
        
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            // Output the final result to folder.
            for (Text tmp_v : values) {
                String line = tmp_v.toString();
                String[] line_splited = line.split(":");
                // if the dis between two node is infinity, skip it.
                if(Double.parseDouble(line_splited[0]) == Double.MAX_VALUE)
                    continue;
                
                String result =  
                 line_splited[0]+"\t"+line_splited[1];
                
                context.write(new Text(key.toString()), new Text(result));
                
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        IN = args[0];
        OUT = args[1];
        Source_Point = args[2];
        
        String input = IN;
        String output = OUT + System.nanoTime();
        
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SingleSourceSP");
        job.setJarByClass(SingleSourceSP.class);
        
        job.setMapperClass(FirstMapper.class);
        job.setReducerClass(FirstReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
        
        String hostnamePort = "hdfs://localhost:9000";
        
        boolean isdone = false;
        while(!isdone){
            
            input = output;
            output = OUT + System.nanoTime();
            
            conf = new Configuration();
            job = Job.getInstance(conf, "SingleSourceSP");
            job.setJarByClass(SingleSourceSP.class);
            
            job.setMapperClass(SRMapper.class);
            job.setReducerClass(SRReducer.class);
            
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            
            job.waitForCompletion(true);
            
            FileSystem hdfs = FileSystem.get(
                                             URI.create(hostnamePort), conf);
            if (hdfs.exists(new Path(input))) {
                hdfs.delete(new Path(input), true);
            }
            
            
            Counters ct = job.getCounters(); 
            if (ct.findCounter(MODIFY_COUNTER.UPDATE).getValue() == 0) {
                isdone = true;
            }
        }
        
        
        conf = new Configuration();
        job = Job.getInstance(conf, "SingleSourceSP");
        job.setJarByClass(SingleSourceSP.class);
        
        job.setMapperClass(EndMapper.class);
        job.setReducerClass(EndReducer.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        
        FileInputFormat.addInputPath(job, new Path(output));
        FileOutputFormat.setOutputPath(job, new Path(OUT));
        job.waitForCompletion(true);
        
        FileSystem hdfs = FileSystem.get(URI.create(hostnamePort),
                                         conf);
        if (hdfs.exists(new Path(output))) {
            hdfs.delete(new Path(output), true);
        }
    }
}