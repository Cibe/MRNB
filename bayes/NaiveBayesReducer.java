package bayes;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.math.stat.descriptive.moment.Variance;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class NaiveBayesReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, NullWritable,Text>{

	String continousVariables,discreteVariables;	
	int targetVariable;
	HashSet<Integer> continousVariablesIndex;
	HashSet<Integer> discreteVariablesIndex;
	
	public HashSet<Integer> splitvariables(String varString){
		HashSet<Integer> hs = new HashSet<Integer>();
	    StringTokenizer tok = new StringTokenizer(varString,",");
	    while(tok.hasMoreElements())
	    	hs.add(Integer.parseInt(tok.nextToken()));
		return hs;
	}
	
	@Override
	 public void configure(JobConf conf){
		continousVariables = conf.get("continousVariables");
		discreteVariables = conf.get("discreteVariables");
	    targetVariable = conf.getInt("targetVariable",0);
	    discreteVariablesIndex = new HashSet<Integer>();
	    continousVariablesIndex = new HashSet<Integer>();
	    if(continousVariables!=null)
	     continousVariablesIndex = splitvariables(continousVariables);
	    if(discreteVariables!=null)
	     discreteVariablesIndex = splitvariables(discreteVariables);
	    }
	
	@Override
	public void reduce(Text keyId, Iterator<DoubleWritable> values,
			OutputCollector<NullWritable, Text> output, Reporter arg3) throws IOException {
		// TODO Auto-generated method stub
		String id = keyId.toString().split("_")[0];
		if(continousVariablesIndex.contains(Integer.parseInt(id))){
			double sumsqr=0,sum = 0,count=0,tmp;
			double mean,var;
			 while (values.hasNext())
	          {
	        	   tmp=values.next().get();
	        	   sumsqr+=tmp*tmp;
	               sum += tmp;
	               count++;
	          }
			 mean=sum/count;
			 var=(sumsqr-((sum*sum)/count))/count;
	         output.collect(NullWritable.get(), new Text(keyId+" "+mean+","+var));
		}
		if(discreteVariablesIndex.contains(Integer.parseInt(id))){
			Double count = 0.0;
			while (values.hasNext())
	          count +=  values.next().get();
			  output.collect(NullWritable.get(), new Text(keyId+" "+count.toString()));
		}
		if(targetVariable == Integer.parseInt(id)){
			Double count = 0.0;
			while (values.hasNext())
	          count +=  values.next().get();
			  output.collect(NullWritable.get(), new Text(keyId+" "+count.toString()));
		}
	}
}
