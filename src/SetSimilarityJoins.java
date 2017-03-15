package homework_2;



import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.hash.HashCode;



public class SetSimilarityJoins extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new SetSimilarityJoins(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "SetSimiliarityJoins");
      job.setJarByClass(SetSimilarityJoins.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setNumReduceTasks(1);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
      FileSystem fs = FileSystem.newInstance(getConf());

      if (fs.exists(new Path(args[1]))) {
		fs.delete(new Path(args[1]), true);
	}
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.getConfiguration().set("mapreduce.output.textoutputformat.seperator", "->");
      

      
      job.waitForCompletion(true);
      
      System.out.println("Comparisons made : "+Reduce.antall);
      System.out.println("Similar docs : "+Reduce.like);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
 

      private static IntWritable counter = new IntWritable(0);
      
      protected void setup(Context context) throws IOException, InterruptedException { 
      File inputFile = new File("/home/cloudera/workspace/Homework1b/PreProcessing.txt");
	  BufferedReader read = new BufferedReader(new FileReader(inputFile));
 
	  String ord = null;
	  while ((ord = read.readLine()) != null){
		  counter.set(counter.get()+1);
		  
	  }
	  read.close();
      }
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
    	  Integer linjenummer = Integer.parseInt(value.toString().trim().split(";")[0]);
    	  String setning = (value.toString().split(";")[1]);
    	  
    	  

    	  for (Integer i=0;i<counter.get();i++) {
    		  if  (i != linjenummer){
    			  	Integer minId = Math.min(i, linjenummer);
  	 				Integer maxId = Math.max(i, linjenummer);
  	 				String combo = Integer.toString(minId)+","+Integer.toString(maxId);
  	 				context.write(new Text(combo), new Text(setning) );
    		  	}
    		  
    	  }
      }
   }
      
   
   

   public static class Reduce extends Reducer<Text, Text, Text, Text> {
	   private static Integer like = 0;
	   private static Integer antall = 0;
	      

      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
    	  HashSet<String> unik = new HashSet<String>();
    	  
    	  float total = 0;
    	  float lik = 0;
    	  float likhet = 0;
    	  for (Text verdi : values) {
    		  for (String enkeltOrd : verdi.toString().split(",")){
	    			 if(unik.contains(enkeltOrd)){
	    				 lik++;
	    			 }
	    			 else {
	    				 unik.add(enkeltOrd);
    		  }
    		  
    	  }
    	  }
    	  total = unik.size();
    	  likhet = lik/total;
    	  
    	  if (likhet > 0.8){
    		  context.write(key, new Text(Float.toString(likhet)));  
    		  like++;
    	  }
    	  
    	  
 
    	 

         antall++;	
         		

         		}
      
   }
}
