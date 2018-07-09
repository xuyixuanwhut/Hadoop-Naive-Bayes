package org.xuchenlian.project.bank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class NaiveMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		 String[] fields = value.toString().split(";");
		 String age = fields[0];
		 if (!age.contains("\"")) {
				String duration = fields[10];
				String campaign = fields[11];
   			    String pdays = fields[12];
				String previous = fields[13];
				String emp_var_rate = fields[15];
				String cons_price_idx = fields[16];
				String cons_conf_idx = fields[17];
				String euribor3m = fields[18];
				String nr_employed = fields[19];
context.write(new Text("age"), new DoubleWritable(Double.parseDouble(age)));
context.write(new Text("duration"), new DoubleWritable(Double.parseDouble(duration)));
context.write(new Text("campaign"), new DoubleWritable(Double.parseDouble(campaign)));
context.write(new Text("pdays"), new DoubleWritable(Double.parseDouble(pdays)));
context.write(new Text("previous"), new DoubleWritable(Double.parseDouble(previous)));
context.write(new Text("emp_var_rate"), new DoubleWritable(Double.parseDouble(emp_var_rate)));
context.write(new Text("cons_price_idx"), new DoubleWritable(Double.parseDouble(cons_price_idx)));
context.write(new Text("cons_conf_idx"), new DoubleWritable(Double.parseDouble(cons_conf_idx)));
context.write(new Text("euribor3m"), new DoubleWritable(Double.parseDouble(euribor3m )));
context.write(new Text("nr_employed"), new DoubleWritable(Double.parseDouble(nr_employed)));
			}
	}
}
