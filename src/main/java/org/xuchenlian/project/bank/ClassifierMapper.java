package org.xuchenlian.project.bank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ClassifierMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	DoubleWritable one = new DoubleWritable(1);

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split(";");
		String age = fields[0];

		if (!age.contains("\"")) {
			int classIndex = fields.length - 1;
			String theClass = fields[classIndex];
			for (int i = 0; i < (classIndex - 1); i++) {
				if (fields[i].contains("\"")) {
					if (!(fields[i].equals("\"unknown\""))) {
						String reduceKey = fields[i] + "," + theClass;
						if (fields[i].substring(1, fields[i].length() - 1).equalsIgnoreCase("yes")
								|| fields[i].substring(1, fields[i].length() - 1).equalsIgnoreCase("no")) {
							context.write(new Text(i + reduceKey), one);
						} else

						{
							context.write(new Text(reduceKey), one);
						}
					} else {
						// System.out.println(fields[i]);
					}
				}

			}

			String reduceKey = "Class," + theClass;
			context.write(new Text(reduceKey), one);
		}
	}

}
