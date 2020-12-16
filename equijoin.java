
import java.util.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.lang.*;

import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.*;

public class equijoin
{


public static class EquijoinMapper extends  Mapper <Object, Text, Text, Text>
{
		
public void map(Object key, Text value, Context con) throws IOException, InterruptedException {
	
String values = value.toString();
String metadata[] = values.split(",");

String key_1 = metadata[1].trim();


int contents = metadata.length;

StringJoiner ER1 = new StringJoiner("");
for(int b = 0; b < contents; b++)
{
ER1.add(metadata[b]);
ER1.add(", ");

}
String entireRow = ER1.toString();


con.write(new Text(key_1), value);
}
}


public static class EquijoinReducer extends Reducer<Text, Text, Text, Text>
{


public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException
{
System.out.println("kavya");
Text finalView = new Text();
String fView = "";
String sView = "";
String fIndex = "";
String sIndex = "";
int i=0;
ArrayList <String > ft = new ArrayList<String>();
ArrayList <String > st = new ArrayList<String>();

for(Text value : values) {
	System.out.println("lol");

	String MD1[] = value.toString().split(",");
	if(i==0) {
		
		fIndex = MD1[0];
		
		ft.add(value.toString());
		i=1;
		continue;
	}
	
	if(!fIndex.equals(MD1[0])) {
		
	   sIndex = MD1[0];
	   
	   st.add(value.toString());
	   
	}
	else {
		
		
		ft.add(value.toString());
		
	}
	
	i++;
}


String view = "";
if(ft.size()!=0 && st.size()!=0) {

for(int x =0 ; x < ft.size(); x++)
{
	
    String t1 = ft.get(x);  

  for(int y =0 ; y < st.size(); y++)
  {
	  String t2 = st.get(y);
	  
      view = t1 + ", " + t2;
      finalView.set(view);
      Text e = new Text();
      String emp = "";
      e.set(emp);		  
      con.write(e, finalView); 
    
   }  
  }
 }
}
}




public static void main(String[] args) throws Exception
{


    Configuration config = new Configuration();
    Job job = Job.getInstance(config, "equijoin");
    job.setJarByClass(equijoin.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapperClass(EquijoinMapper.class);
    job.setReducerClass(EquijoinReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}