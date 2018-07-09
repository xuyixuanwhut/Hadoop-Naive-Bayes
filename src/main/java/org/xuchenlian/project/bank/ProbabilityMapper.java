package org.xuchenlian.project.bank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProbabilityMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\t");
		String k = fields[0];
		String v = fields[1];
		String[] k_p = k.split(",");
		String part_key = k_p[1];
		String mark = k_p[0];
		if(!mark.equals("Class")) {
		if (part_key.equals("\"yes\"")) {
			context.write(new Text(k), new DoubleWritable(Double.parseDouble(v.toString())/4640.0));
		}else if(part_key.equals("\"no\"")) {
			context.write(new Text(k), new DoubleWritable(Double.parseDouble(v.toString())/36548.0));
		}
		}else {
			context.write(new Text("yes"),new DoubleWritable(4640.0/(36548.0+4640.0)));
			context.write(new Text("no"),new DoubleWritable(36548.0/(36548.0+4640.0)));
		}
	}
}
