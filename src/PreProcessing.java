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



public class PreProcessing extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new PreProcessing(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "PreProcessing");
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");
      job.setJarByClass(PreProcessing.class);
      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(Text.class);
      job.setNumReduceTasks(1);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.addInputPath(job, new Path("pg100.txt")); 
      Path outputPath = new Path("output/PreProcessing");
      FileOutputFormat.setOutputPath(job, outputPath);
      FileSystem fs = FileSystem.get(getConf());
      if (fs.exists(outputPath)){
	      fs.delete(outputPath, true);
	}
      
      //FileInputFormat.addInputPath(job, new Path(args[0]));
      //FileOutputFormat.setOutputPath(job, new Path(args[1]));

      
      

      
      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
      private Text word = new Text();
      private static LongWritable SN = new LongWritable(0);
      private static TreeMap <String, Integer> RF = new TreeMap<String, Integer>();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  String line = new String();
    	  File inputFile = new File("/home/cloudera/workspace/Homework1/output/StopWords_final_1.txt");
    	  BufferedReader read = new BufferedReader(new FileReader(inputFile));
    	  
    	  
    	  Set<String> SW = new HashSet<String>();


    	  String ord = null;
    	  while ((ord = read.readLine()) != null){
    		  SW.add(ord);
    	  }
    	  read.close();
    	 
    	  String input = value.toString();
    	  if (!input.isEmpty()) {
    	  for (String token: value.toString().replaceAll("[^0-9A-Za-z ]","").split("\\s+")){
    		  if ((!SW.contains(token)) && !token.isEmpty()){
    			  word.set(token.trim());
    			  
    		 
    		  
    			  context.write(SN, word); 
        	 
    			  if (RF.containsKey(token)) {
    				  RF.put(token, RF.get(token)+1);
    			  	}
    			  else {
    				  RF.put(token, 0);
    			  }
    		  }
    	  }
    	  SN.set(SN.get()+1);  
      }
   
   }
   }

   public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
		  HashMap <String, Integer> counter = new HashMap<String, Integer>();
	      
	      protected void setup(Context context) throws IOException, InterruptedException { 
	          counter.put("lines", 0);
	       }
      @Override
      public void reduce(LongWritable key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {

         String line = new String();
    	 HashSet<String> rdw = new HashSet<String>();
    	 SortedMap <Integer, String> sorted = new TreeMap<Integer, String>();
    	 
    	 for (Text verdi : values) {
    		 if (!rdw.contains(verdi.toString())) {
    			 rdw.add(verdi.toString());
    		 }
    	 }
    	 
    	 for (String verdi : rdw) {
    		 sorted.put(Map.RF.get(verdi),verdi);
    	 }
    
    	 
    
         for (String word : sorted.values()) {
        	 line += word+",";
         }
         		line = line.substring(0, line.length()-1);
         	
         		context.write(key, new Text(line));
         		counter.put("lines",counter.get("lines")+1);
         		}
      
      protected void cleanup(Context context) throws IOException, InterruptedException {
	       try{
	          Path pt=new Path("output/preprocessing/Counter.txt");
	          FileSystem fs = FileSystem.get(new Configuration());
	          BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
	          String line1="Number of lines: " + counter.get("lines") + "\n";
	          br.write(line1);
	          br.close();
	    }catch(Exception e){
	            System.out.println("Error");
         	}
        
      }
   }
}
