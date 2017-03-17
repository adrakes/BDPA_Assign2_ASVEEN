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



public class SetSimilarityJoinsB extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new SetSimilarityJoinsB(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "SetSimiliarityJoins");
      job.setJarByClass(SetSimilarityJoinsB.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
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
   
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
 
	  private static TreeMap<Integer, String> ordbok = new TreeMap<Integer, String>();

      
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
    	  Integer linjenummer = Integer.parseInt(value.toString().trim().split(";")[0]);
    	  String setning = (value.toString().split(";")[1]);
    	  Integer lengde = setning.split(",").length;
    	  double Jaccard = 0.8;
    	  
    	  if (!ordbok.keySet().contains(linjenummer)){
    		  ordbok.put(linjenummer,setning);
    	  }
    	  Integer nyLengde = lengde-(int)Math.ceil(Jaccard*(lengde))+1;
    	  for (Integer i=0;i<nyLengde;i++){
    		  String ord = setning.split(",")[i];
  	 			context.write(new Text(ord), new IntWritable(linjenummer));
    	  }
      }
   }
      	
 
   public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
	   private static Integer like = 0;
	   private static Integer antall = 0;
	   HashSet<String> unique = new HashSet<String>();  

      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
    	  HashSet<Integer> unik = new HashSet<Integer>();
    	  
    	  
    	  float total = 0;
    	  float lik = 0;
    	  float likhet = 0;
    	  Integer D1 = 0;
    	  Integer D2 = 0;
    	  
    	  for (IntWritable verdi: values){
    		  unik.add(verdi.get());
    	  }
    	  //System.out.println(unik);
    	  //System.out.println(key);
    	 
    	  for (Integer verdi : unik) {
    		  D1 = verdi;
    		  for (Integer verdi2 : unik) {
    			  D2 = verdi2;
    			  //System.out.println(D1+","+D2);
    			  if (D1 != D2 && !unique.contains(Math.min(D1,D2)+","+Math.max(D1,D2))) {
    				  HashSet<String> loop = new HashSet<String>();
    				  total = 0;
    				  lik = 0;
    				  likhet = 0;
    				  for (String linje : Map.ordbok.get(D1).toString().split(",")){
    					  loop.add(linje);
    				  }
    				  
    				  for(String enkeltord : Map.ordbok.get(D2).toString().split(",")){
    					  if (loop.contains(enkeltord)){
    						  lik++;
    					  }
    					  else {
    						  loop.add(enkeltord);
    					  }
    					  }
    				  total = loop.size();
    				  if (total !=0){
    					  likhet = lik/total;
    					  }
    				  String combo = D1+","+D2; 
    				  if (likhet > 0.8){
    					context.write(new Text(combo), new Text(Float.toString(likhet)));  
    					like++;
    	  }
    			  
    	 unique.add((Math.min(D1,D2)+","+Math.max(D1,D2)));
         antall++;	
         		
	
    			  
	      
    		  }
    	  }
      }
   }
   	}

}