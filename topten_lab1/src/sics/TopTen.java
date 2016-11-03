package sics;

import java.io.IOException;
import java.util.StringTokenizer;

import java.util.TreeMap;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopTen {

	public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
//		Stores a map of user reputation to the record
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
		
		private static final String USER_ID = "Id";
		private static final String USER_NAME = "DisplayName";
		private static final String USER_AGE = "Age";
		private static final String USER_ABOUT = "AboutMe";
		private static final String REPUTATION = "Reputation";
		
		

		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String row = value.toString();
			if(row.contains("row")) {
				Map<String, String> parsed = parseInputRow(row);
				String userId = parsed.get(USER_ID);				
				String userName = parsed.containsKey(USER_NAME) ?  parsed.get(USER_NAME) : "NotGiven";
				String userAge = parsed.containsKey(USER_AGE) ?  parsed.get(USER_AGE) : "NotGiven";
				String userAbout = parsed.containsKey(USER_ABOUT) ?  parsed.get(USER_ABOUT) : "NotGiven";
				Integer reputation = Integer.valueOf(parsed.get(REPUTATION));	
				Text userInfo = new Text(reputation + "# Id: " + userId + 
											", Name: " + userName + 
											", Age: " + userAge +
											", About: " + userAbout);
				
//				Add this record to our map with the reputation as the key
				repToRecordMap.put(reputation, userInfo);

//				If we have more than ten records, remove the one with the lowest reputation.
			    if (repToRecordMap.size() > 10) {
//			    	Remove first element as it is the lowest value, so that top ten remain in repToRecordMap
			    	repToRecordMap.remove(repToRecordMap.firstKey());
			    }
			}

		}
		
		/*
		 * Filter out XML 'row' and parsed its attributes (attribute name and value)
		 * into a HashMap
		 * 
		 */
		private Map<String, String> parseInputRow(String row) {
			Map<String, String> parsed = new HashMap<String, String>();
			
//			remove all xml characters and replace whitespace with _
			String cleansedRow = row.replace("<row ", "").replace("\" ", "_").replace("\"", "").replace("/>", "");
			
//			Tokenize cleansedRow with _ character, so that tokenize string will be in format 'attributeName=attributeValue;
			StringTokenizer itr = new StringTokenizer(cleansedRow, "_");
			String token;
			while (itr.hasMoreTokens()) {
				token = itr.nextToken();
//				Consider only string tokens with character =
				if(token.contains("=")) {
					String[] propVal = token.split("=");
					
//					Consider only attributes with a value
					if(propVal.length == 2) {
						parsed.put(propVal[0].trim(), propVal[1].trim());
					}
				}
			}
			
			return parsed;
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
//			Output our ten records to the reducers with a null key
			for (Text t : repToRecordMap.values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}


	
	public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
//		Stores a map of user reputation to the record
//		Overloads the comparator to order the reputations in descending order
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
		
		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				String reduceValue = value.toString();
				String[] data = reduceValue.split("#");
				repToRecordMap.put(Integer.valueOf(data[0]), new Text(data[1]));
				
//				If we have more than ten records, remove the one with the lowest reputation
				if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}
			
			for (Text t : repToRecordMap.descendingMap().values()) {
//				Output our ten records to the file system with a null key
				context.write(NullWritable.get(), t);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Top Ten");
		job.setJarByClass(TopTen.class);
		
		
		job.setMapperClass(TopTenMapper.class);
		job.setReducerClass(TopTenReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
