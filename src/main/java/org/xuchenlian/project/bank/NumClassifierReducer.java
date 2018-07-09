package org.xuchenlian.project.bank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NumClassifierReducer extends Reducer<Text, DoubleWritable, Text, Text> {
StringBuilder doublePair = new StringBuilder();
	List<Double> l = new ArrayList<Double>();

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		double total = 0;
		int count = 0;
		l.clear();
		StringBuilder doublePair = new StringBuilder();
		for (DoubleWritable value : values) {
			l.add(value.get());
			count += 1;
			total += value.get();
		}

		double ave = total / count;
		Iterator<Double> itr = l.iterator();
		double sum = 0;
		while (itr.hasNext()) {
			double val = itr.next();
			sum += Math.pow((val - ave), 2);
		}
		double var = Math.sqrt(sum /count);
		doublePair.append(ave).append("\t").append(var);
		context.write(key, new Text(doublePair.toString()));
	}

}