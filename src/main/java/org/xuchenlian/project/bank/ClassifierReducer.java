package org.xuchenlian.project.bank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class ClassifierReducer extends
        Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	

 
	  public void reduce(Text text, Iterable<DoubleWritable> values, Context context)
	            throws IOException, InterruptedException {
	int total=0;
	        for (DoubleWritable value : values) {
	            total+= value.get();
	       
	        }
	        
	         
	     
	        
	        context.write(text, new DoubleWritable(total));
	    }
}