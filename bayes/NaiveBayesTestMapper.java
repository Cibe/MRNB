package bayes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.metrics2.sink.FileSink;
import org.apache.hadoop.fs.Path;

public class NaiveBayesTestMapper extends MapReduceBase implements Mapper<LongWritable, Text,NullWritable, Text>{
	String delimiter,continousVariables,discreteVariables,targetClasses;	
	int targetVariable,numColums;
	HashSet<Integer> continousVariablesIndex;
	HashSet<Integer> discreteVariablesIndex;
	HashMap<String,String> hm;
	HashSet<String> classesTargetVariables;
	
	public HashSet<Integer> splitvariables(String varString){
		HashSet<Integer> hs = new HashSet<Integer>();
	    StringTokenizer tok = new StringTokenizer(varString,",");
	    while(tok.hasMoreElements())
	    	hs.add(Integer.parseInt(tok.nextToken()));
		return hs;
	}
	public HashSet<String> splitstringvariables(String varString){
		HashSet<String> hs = new HashSet<String>();
	    StringTokenizer tok = new StringTokenizer(varString,",");
	    while(tok.hasMoreElements())
	    	hs.add(tok.nextToken());
		return hs;
	}
	
	@Override
	 public void configure(JobConf conf){
		delimiter = conf.get("delimiter");
		numColums = conf.getInt("numColumns", 0);
		continousVariables = conf.get("continousVariables");
		discreteVariables = conf.get("discreteVariables");
		targetClasses = conf.get("targetClasses");
	    targetVariable = conf.getInt("targetVariable",0);
	    discreteVariablesIndex = new HashSet<Integer>();
	    continousVariablesIndex = new HashSet<Integer>();
	    if(continousVariables!=null)
	    continousVariablesIndex = splitvariables(continousVariables);
	    if(discreteVariables!=null)
	    discreteVariablesIndex = splitvariables(discreteVariables);
	    classesTargetVariables = splitstringvariables(targetClasses);
	    
	    hm = new HashMap();
	    try {
			URI[] filesIncache = DistributedCache.getCacheFiles(conf);
			for(int i=0;i<filesIncache.length;i++){
				BufferedReader fis = new BufferedReader(new FileReader(filesIncache[i].getPath().toString()));
				String record; 
				 while ((record = fis.readLine()) != null) {
					 String key,value;
					 StringTokenizer tokRecord = new StringTokenizer(record);
					 key = tokRecord.nextToken();
					 value = tokRecord.nextToken();
					 hm.put(key, value);
				 }			
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	double calculateProbablity(int featureID,String value,String label){
	   String classCount,valueCount,totalCount;
	   classCount = hm.get(targetVariable+"_"+label);
	   if(classCount==null)
		   return 1.0;
	   valueCount = hm.get(featureID+"_"+value+"_"+label);
	   if(valueCount==null)
		   return 1.0;
	   totalCount = hm.get(targetVariable+"");
	   double classProbablity = (Double.parseDouble(classCount) / Double.parseDouble(totalCount));
	   double valueProbablity = (Double.parseDouble(valueCount) / Double.parseDouble(classCount));
	   return (classProbablity*valueProbablity);
	}
	
	double calculateGaussian(int featureID,String value,String label){
		Double mean,variance,val;
		val = Double.parseDouble(value);
		String values = hm.get(featureID+"_"+label);
		if(values!=null){
	      StringTokenizer tokMeanVariance = new StringTokenizer(values,",");
	      mean = Double.parseDouble(tokMeanVariance.nextToken());
	      variance = Double.parseDouble(tokMeanVariance.nextToken());
	      if(variance==0.0)
	    	  return 1.0;
	      double exponent,denaminator;
	      denaminator = Math.sqrt(2*3.414)*variance;
	      exponent = -1*(Math.pow((val-mean),2))/(2*Math.pow(variance, 2));
	      return (1/denaminator)*Math.exp(exponent);
		}
		return 1.0;
	}
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<NullWritable, Text> output, Reporter arg3) throws IOException {
		// TODO Auto-generated method stub 
		String record = value.toString();
	    int featureID = 1;
	    Double probablity=0.0;
	    Double labelProbablity[] = new Double[classesTargetVariables.size()];
	    String features[] = record.split(delimiter);
	    int labelIndex = 0;
	    String labelprobablityString="";
	    for (String labels : classesTargetVariables){
	    	probablity=1.0;
	    	featureID = 1;
	      for(int i=0; i<numColums; i++){
	    	if(discreteVariablesIndex.contains(featureID)){
	    	   probablity = probablity * calculateProbablity(featureID,features[i],labels);
	    	}
	    	if(continousVariablesIndex.contains(featureID)){
	    	  probablity = probablity * calculateGaussian(featureID,features[i],labels);
	    	}
	    	++featureID;
	    		//output.collect(new Text(featureID+"_"+features[targetVariable-1]), new DoubleWritable(Double.parseDouble(features[i])));	
	     }
	     labelProbablity[labelIndex++]=probablity;
	     labelprobablityString = labelprobablityString + labelProbablity[labelIndex-1]+" ";
	   }
	   output.collect(NullWritable.get(),new Text(record+" "+labelprobablityString));
	}
}
