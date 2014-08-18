
package bayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class NaiveBayesTrainJob extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		JobConf conf = new JobConf(getConf(),NaiveBayesTrainJob.class);
		conf.setJobName("Training");
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(DoubleWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setNumMapTasks(conf.getInt("numMaps", 4));
		conf.setNumReduceTasks(conf.getInt("numReduce", 1));
		conf.setMapperClass(NaiveBayesMapper.class);
		conf.setReducerClass(NaiveBayesReducer.class);
		FileInputFormat.addInputPath(conf, new Path(conf.get("input")));
		FileOutputFormat.setOutputPath(conf, new Path(conf.get("output")));
		JobClient.runJob(conf);
		return 0;
	}
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new NaiveBayesTrainJob(), args);
		System.exit(res);
	}
}
