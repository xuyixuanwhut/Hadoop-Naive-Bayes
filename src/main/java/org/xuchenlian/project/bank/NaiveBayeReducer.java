package org.xuchenlian.project.bank;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NaiveBayeReducer extends Reducer<Text, Text, Text, Text> {
	Map<String, String> probTable = new HashMap<String, String>();

	public void setup(Context context) throws IOException, InterruptedException {
		String mark;
		String val1;
		String val2;

		try {
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());

			if (files != null && files.length > 0) {

				for (Path file : files) {
					try {
						File myFile = new File(file.toUri());
						BufferedReader bufferedReader = new BufferedReader(new FileReader(myFile.toString()));
						String line = null;
						while ((line = bufferedReader.readLine()) != null) {

							String[] split = line.split("\\s+");
							if (split.length == 3) {
								mark = split[0];
								val1 = split[1];
								val2 = split[2];
								probTable.put(mark, val1 + "," + val2);
								// System.out.println(mark+","+val1+","+val2);
							} else if (split.length == 2) {
								mark = split[0];
								val1 = split[1];
								probTable.put(mark, val1);
							} else if (split.length > 3) {
								int index = line.lastIndexOf("@");
								String[] val = line.substring(index + 1, line.length() - 1).split("\\s+");
								probTable.put(val[0], val[1] + "," + val[2]);
								// System.out.println(val[0]);

							}

						}
					} catch (Exception e) {
					}
				}
			}

		} catch (Exception e) {
		}
		// System.out.println(probTable.get("cons_conf_idx"));

	}

	// double yes = Double.parseDouble(probTable.get("yes"));
	// double no = Double.parseDouble(probTable.get("no"));
	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

		String[] fields = key.toString().split(";");
		String ages = fields[0];
		double yes = Double.parseDouble(probTable.get("yes"));
		double no = Double.parseDouble(probTable.get("no"));
		double prob_yes = yes;
		double prob_no = no;
		if (!ages.contains("\"")) {

			double age = Double.parseDouble(ages);
			double duration = Double.parseDouble(fields[10]);
			double campaign = Double.parseDouble(fields[11]);
			double pdays = Double.parseDouble(fields[12]);
			double previous = Double.parseDouble(fields[13]);
			double emp_var_rate = Double.parseDouble(fields[15]);
			double cons_price_idx = Double.parseDouble(fields[16]);
			double cons_conf_idx = Double.parseDouble(fields[17]);
			double euribor3m = Double.parseDouble(fields[18]);
			double nr_employed = Double.parseDouble(fields[19]);
			if (!(duration == 0)) {
				for (int i = 0; i < 20; i++) {

					if (i == 0) {
						double cal_yes = prob_cal_yes("age", age);
						double cal_no = prob_cal_no("age", age);
						prob_yes = prob_yes * cal_yes;
						prob_no = prob_no * cal_no;

					} else if (i == 10) {
						double dur = Double.parseDouble(fields[10]);

						double cal_yes = prob_cal_yes("duration", duration);
						double cal_no = prob_cal_no("duration", duration);
						prob_yes = prob_yes * cal_yes;
						prob_no = prob_no * cal_no;

					} else if (i == 11) {
						double cal_yes = prob_cal_yes("campaign", campaign);
						double cal_no = prob_cal_no("campaign", campaign);
						prob_yes = prob_yes * cal_yes;
						prob_no = prob_no * cal_no;

					} else if (i == 13) {
						double cal_yes = prob_cal_yes("previous", previous);
						double cal_no = prob_cal_no("previous", previous);
						prob_yes = prob_yes * cal_yes;
						prob_no = prob_no * cal_no;

					} else if (i == 15) {
						double cal_yes = prob_cal_yes("emp_var_rate", emp_var_rate);
						double cal_no = prob_cal_no("emp_var_rate", emp_var_rate);
						prob_yes = prob_yes * cal_yes;
						prob_no = prob_no * cal_no;

					} else if (i == 16) {
						double cal_yes = prob_cal_yes("cons_price_idx", cons_price_idx);
						double cal_no = prob_cal_no("cons_price_idx", cons_price_idx);
						prob_yes = prob_yes * cal_yes;
						prob_no = prob_no * cal_no;

					} else if (i == 17) {

						double cal_yes = prob_cal_yes("cons_conf_idx", cons_conf_idx);
						double cal_no = prob_cal_no("cons_conf_idx", cons_conf_idx);
						prob_yes = prob_yes * cal_yes;
						prob_no = prob_no * cal_no;

					} else if (i == 18) {
						double cal_yes = prob_cal_yes("euribor3m", euribor3m);
						double cal_no = prob_cal_no("euribor3m", euribor3m);
						prob_yes = prob_yes * cal_yes;
						prob_no = prob_no * cal_no;

					} else if (i == 19) {
						double cal_yes = prob_cal_yes("nr_employed", nr_employed);
						double cal_no = prob_cal_no("nr_employed", nr_employed);
						prob_yes = prob_yes * cal_yes;
						prob_no = prob_no * cal_no;

					} else if (i == 4 || i == 5 || i == 6) {
						String i_string = i + "";
						String get_prob_no = probTable.get(i_string + fields[i] + ",\"no\"");
						String get_prob_yes = probTable.get(i_string + fields[i] + ",\"yes\"");

						if (!(get_prob_no == null)) {
							double get_prob_no_num = Double.parseDouble(get_prob_no);
							prob_no = prob_no * get_prob_no_num;

						}
						if (!(get_prob_yes == null)) {
							double get_prob_yes_num = Double.parseDouble(get_prob_yes);
							prob_yes = prob_yes * get_prob_yes_num;
						}

					} else if (i == 1 || i == 2 || i == 3 || i == 7 || i == 8 || i == 9 || i == 14) {
						String get_prob_no = probTable.get(fields[i] + ",\"no\"");
						String get_prob_yes = probTable.get(fields[i] + ",\"yes\"");
						if (!(get_prob_no == null)) {
							double get_prob_no_num = Double.parseDouble(get_prob_no);
							prob_no = prob_no * get_prob_no_num;
						} else if (fields[i].equals("\"unknown\"")) {
							prob_no = prob_no * 1;
						} else {
							prob_no = 0;

						}
						if (!(get_prob_yes == null)) {
							double get_prob_yes_num = Double.parseDouble(get_prob_yes);
							prob_yes = prob_yes * get_prob_yes_num;
						} else if (fields[i].equals("\"unknown\"")) {
							prob_yes = prob_yes * 1;
						} else {
							prob_yes = 0;
						}

					}
				}
				// System.out.println(prob_no);

				if (prob_yes > prob_no) {
					context.write(key, new Text("\"yes\""));
				} else if (prob_no > prob_yes) {
					context.write(key, new Text("\"no\""));
				} else {
					context.write(key, new Text("not decide"));
				}
			} else {
				context.write(key, new Text("\"no\""));
			}
		}
	}

	public double prob_cal_yes(String s1, double s2) {
		double mean = Double.parseDouble(probTable.get("\"yes\"" + s1).split(",")[0]);
		double var = Double.parseDouble(probTable.get("\"yes\"" + s1).split(",")[1]);
		if (var == 0)
			return 0;
		else {
			double prob_cal_yes = Math.pow(Math.E, -Math.pow(s2 - mean, 2) / (2 * Math.pow(var, 2)))
					/ (var * Math.sqrt(2 * Math.PI));
			return prob_cal_yes;
		}
	}

	public double prob_cal_no(String s1, double s2) {
		double mean = Double.parseDouble(probTable.get("\"no\"" + s1).split(",")[0]);
		double var = Double.parseDouble(probTable.get("\"no\"" + s1).split(",")[1]);
		if (var == 0)
			return 0;
		else {
			double prob_cal_no = Math.pow(Math.E, -Math.pow(s2 - mean, 2) / (2 * Math.pow(var, 2)))
					/ (var * Math.sqrt(2 * Math.PI));
			return prob_cal_no;
		}
	}
}
