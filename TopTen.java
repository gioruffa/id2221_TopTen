package topten;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class TopTen {
    // This helper function parses the stackoverflow into a Map for us.
  public static Map<String, String> transformXmlToMap(String xml) {
    Map<String, String> map = new HashMap<String, String>();
    try {
        String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
        for (int i = 0; i < tokens.length - 1; i += 2) {
    	String key = tokens[i].trim();
    	String val = tokens[i + 1];
    	map.put(key.substring(0, key.length() - 1), val);
        }
    } catch (StringIndexOutOfBoundsException e) {
        System.err.println(xml);
    }

    return map;
  }

  public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
  // Stores a map of user reputation to the record
    TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // <FILL IN>
        // System.out.println("HEJ");
        Map<String,String> mappedXML = transformXmlToMap(value.toString());
        // System.out.println(mappedXML.keySet());
        if (mappedXML.containsKey("Id")
            && mappedXML.get("Id") != null
            && mappedXML.containsKey("Reputation")
            && mappedXML.get("Reputation") != null)  // the second one may be an overkill, TODO:check
        {
          Text userID = new Text( mappedXML.get("Id"));
          repToRecordMap.put(Integer.parseInt(mappedXML.get("Reputation")), userID);
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Output our ten records to the reducers with a null key
        // <FILL IN>
        System.out.println(
          String.format("There are %d records in the map", repToRecordMap.size())
          );
        // Text testText = new Text("asd");
        // Get the first 10 by reputation, we iterate through the keyset
        // in descending order
        int emittedCount = 10;
        for (Integer reputation : repToRecordMap.descendingKeySet())
        {
          //Since we do not want to touch the method signatures, we embed
          //both the fields in a text and we separate them with a space
          //hoping that the id does not contain spaces, which is a strong
          //assumption
          String payload = String.format(
            "%d %s", reputation, repToRecordMap.get(reputation)
          );
          System.out.println(String.format("Emitting \"%s\"", payload));
          Text toEmit = new Text(payload);
          context.write(NullWritable.get(), toEmit);

          //ugly but working
          emittedCount--;
          if (emittedCount == 0)
          {
            break;
          }
        }
    }
  }

  public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
  // Stores a map of user reputation to the record
    private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println("Reducing");
        //TODO: IMPORTANT! FILL THE TREE BECAUSE THIS WORKS ONNLY WITH ONE MAPPER!!!
        //TODO: serializer and deserializer from-to-text
        for (Text value : values)
        {
          Integer reputation = Integer.parseInt(value.toString().split(" ")[0]);
          Text userID = new Text(value.toString().split(" ")[1]);
          repToRecordMap.put(reputation, userID);
        }
        int emittedCount = 10;
        for (Integer reputation : repToRecordMap.descendingKeySet())
        {
          // String id = value.toString().split(" ")[1];
          // String reputationString = value.toString().split(" ")[0];
          // System.out.println(String.format("%s %s", id, reputationString));
          // getBytes may return polluted data;
          // see https://coderwall.com/p/cqqeaa/hadoop-s-text-getbytes-is-a-tricky-method
          byte[] id = repToRecordMap.get(reputation).copyBytes();
          Put insHBase = new Put(id);
          insHBase.addColumn(
            Bytes.toBytes("info"),
            Bytes.toBytes("id"),
            id
          );
          //we insert reputation as integer because it is the only way one could
          //do aggregation on it without having to decode the utf8 value.
          insHBase.addColumn(
            Bytes.toBytes("info"),
            Bytes.toBytes("rep"),
            // Bytes.toBytes(String.valueOf(reputation))
            Bytes.toBytes(reputation)
          );
          context.write(null, insHBase);

          emittedCount--;
          if (emittedCount == 0)
          {
            break;
          }
        }


    }
  }

  public static void main(String[] args) throws Exception {
  // <FILL IN>
    Configuration conf = HBaseConfiguration.create();
    Job job = Job.getInstance(conf);
    job.setJarByClass(TopTen.class);
    job.setMapperClass(TopTenMapper.class);
    //do not use job.setOutputKeyClass because it sets
    //the type both for the mapper and the reducer
    //in our case the type handlyng for the reducer is performed
    //by the initTableReducerJob method
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);

    // define output table
    TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    job.setNumReduceTasks(1);
    // 3 is always a good number to see if something works
    // but this does not work because of this: https://wiki.apache.org/hadoop/HowManyMapsAndReduces
    // job.setNumMapTasks(3);
    job.waitForCompletion(true);

  }
}
