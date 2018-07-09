package org.xuchenlian.project.bank;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {
		if (args.length != 7) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <outputpath>");
			System.exit(-1);
		}

		Path inputPath = new Path(args[0]);
		Path job1_output = new Path(args[1]);
		Path job2_output = new Path(args[2]);
		Path job3_output = new Path(args[3]);
		Path hdfsFile=new Path(args[4]);
		Path job4_input=new Path(args[5]);
		Path job4_output=new Path(args[6]);
		// Create configuration
		Configuration conf1 = new Configuration(true);

		// Create job
		Job job1 = new Job(conf1, "bin");
		// ControlledJob cj1 = new ControlledJob(conf);

		job1.setJarByClass(ClassifierMapper.class);

		// job1.setMapperClass(MaxMinMapper.class);
		job1.setMapperClass(ClassifierMapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(DoubleWritable.class);
		// job.setReducerClass(BinReducer.class);
		job1.setReducerClass(ClassifierReducer.class);
		job1.setNumReduceTasks(2);
		// Specify key / value
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);

		// Input
		FileInputFormat.addInputPath(job1, inputPath);
		FileOutputFormat.setOutputPath(job1, job1_output);
		job1.setInputFormatClass(TextInputFormat.class);
		//

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "numclassifier");

		job2.setJarByClass(Driver.class);
		job2.setMapperClass(NumClassifierMapper.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(DoubleWritable.class);
	//	job2.setPartitionerClass(CustomerPartitioner.class);
		job2.setNumReduceTasks(1);
		job2.setReducerClass(NumClassifierReducer.class);
		job2.setOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setInputFormatClass(TextInputFormat.class);

		FileInputFormat.addInputPath(job2, inputPath);
		FileOutputFormat.setOutputPath(job2, job2_output);
		boolean complete=job1.waitForCompletion(true)&&job2.waitForCompletion(true); 
		//
		if(complete) {
		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3, "probabilitytable");
		
		job3.setJarByClass(Driver.class);
		job3.setMapperClass(ProbabilityMapper.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(DoubleWritable.class);
		job3.setNumReduceTasks(0);
		job3.setInputFormatClass(TextInputFormat.class);

		FileInputFormat.addInputPath(job3, job1_output);
		FileOutputFormat.setOutputPath(job3, job3_output);
		boolean complete2 =job3.waitForCompletion(true);
		if(complete2) {
			Configuration conf4 = new Configuration();
			Job job4 = Job.getInstance(conf4, "naive");
			Path[] in_total= {job3_output,job2_output};
			FileSystem local = FileSystem.get(conf4);
			FileSystem hdfs = FileSystem.get(conf4);
			try{
				FileStatus[] inputFiles = local.listStatus(in_total);
				
				FSDataOutputStream out = hdfs.create(hdfsFile);
				for (int i=0; i<inputFiles.length; i++) {
					System.out.println(inputFiles[i].getPath().getName());
					FSDataInputStream in =hdfs.open(inputFiles[i].getPath());
					byte buffer[] = new byte[256];
					int bytesRead = 0;
					while( (bytesRead = in.read(buffer)) > 0) {
						out.write(buffer, 0, bytesRead);
						}
						in.close();
						}
						out.close();
						} catch (IOException e) {
						e.printStackTrace();
						}
			DistributedCache.addCacheFile(new URI(args[4]), job4.getConfiguration());
			job4.setJarByClass(Driver.class);
			job4.setMapperClass(NaiveBayeMapper.class);
			job4.setMapOutputKeyClass(Text.class);
			job4.setMapOutputValueClass(Text.class);
			job4.setNumReduceTasks(1);
			//
			job4.setReducerClass(NaiveBayeReducer.class);
			job4.setOutputKeyClass(Text.class);
			job4.setOutputValueClass(Text.class);
			job4.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(job4, job4_input);
			FileOutputFormat.setOutputPath(job4,job4_output);
			if (hdfs.exists(job4_output))
				hdfs.delete(job4_output, true);
			// Execute job
			int code = job4.waitForCompletion(true) ? 0 : 1;
			System.exit(code);
		}
		}

//		ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());
//		controlledJob1.setJob(job1);
//
//		ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());
//		controlledJob2.setJob(job2);
//
//		ControlledJob controlledJob3 = new ControlledJob(job3.getConfiguration());
//		controlledJob3.setJob(job3);
//		controlledJob3.addDependingJob(controlledJob1);
//		controlledJob3.addDependingJob(controlledJob2);
//		JobControl jc = new JobControl("bank_chain");
//		jc.addJob(controlledJob1);
//		jc.addJob(controlledJob2);
//		jc.addJob(controlledJob3);
//		Thread jcThread = new Thread(jc);
//		jcThread.start();
//
//		if (jc.allFinished()) {
//			System.out.println(jc.getSuccessfulJobList());
//			jc.stop();
//
//		}
//		if (jc.getFailedJobList().size() > 0) {
//			System.out.println(jc.getFailedJobList());
//			jc.stop();

		}

	}
