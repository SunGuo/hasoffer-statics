package com.hasoffer.mr;

import com.hasoffer.util.CommonUtils;
import com.hasoffer.util.LogParserUtil;
import net.sf.json.JSONArray;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hsqldb.lib.StringUtil;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by guoxian1 on 16/5/31.
 */

public class MRStatics extends Configured implements Tool {

    private static class InfoMapper extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
            try {

                Map<String, Object> para = LogParserUtil.getHasofferLog(value.toString().replace("\\x22", "\"").replace("\\x5C", "\\"));
                String deviceId = CommonUtils.getDeviceId(para);
                JSONArray shopApp = (JSONArray) para.get("shopApp");
                String channel = null;

                if(para.containsKey("marketChannel")){
                    channel = (String) para.get("marketChannel");
                }

                String [] values = value.toString().split(" ");

                long cmpPrice = 0;

                if(values.length >=7 && values[6].contains("/cmp/getcmpskus")){
                    cmpPrice = 1;
                }

                if(!StringUtil.isEmpty(deviceId)){
                    if(shopApp != null && shopApp.size() > 0) {
                        if (channel != null) {
                            collector.collect(new Text(deviceId + "," + channel), new Text("1" + "\t" + cmpPrice));
                            collector.collect(new Text(deviceId + "," + "all"), new Text("1" + "\t" + cmpPrice));
                        } else {
                            collector.collect(new Text(deviceId + "," + "all"), new Text("1" + "\t" + cmpPrice));
                        }
                    }
                }

//                String imeiId = null;
//                String channel = null;
//
//                if(para.containsKey("deviceId")){
//                    imeiId = (String) para.get("deviceId");
//                }
//
//                if(para.containsKey("marketChannel")){
//                    channel = (String) para.get("marketChannel");
//                }
//
//                if (imeiId != null && imeiId.trim().length() > 0) {
//                    if (channel != null && channel.trim().length() > 0) {
//                        collector.collect(new Text(imeiId + "," + channel), new Text("1"));
//                        collector.collect(new Text(imeiId + "," + "all"), new Text("1"));
//                    } else {
//                        collector.collect(new Text(imeiId + "," + "all"), new Text("1"));
//                    }
//                }

            } catch (Exception e) {

            }
        }
    }

    private static class CountReducer extends MapReduceBase implements
            Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {

            long allSum = 0;
            long allCmpPrice = 0;
            while (values.hasNext()) {
                String [] vv = values.next().toString().split("\t");
                allSum += Long.parseLong(vv[0].toString());
            }

            String[] keys = key.toString().split(",");
            String imeiId = keys[0];
            String channel = keys[1];
            collector.collect(new Text(channel), new Text("1\t" + allSum));
        }
    }


    private static class CountMapper1 extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {


        public void map(LongWritable key, Text value,
                        OutputCollector<Text, Text> collector, Reporter reporter)
                throws IOException {
            String[] values = value.toString().split("\t");
            collector.collect(new Text(values[0]), new Text(values[1] + "," + values[2]));
        }

    }

    private static class CountReducer1 extends MapReduceBase implements
            Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> collector, Reporter reporter)
                throws IOException {

            long allSum = 0;
            long uniqueSum = 0;

            while (values.hasNext()) {
                String tmp = values.next().toString();
                String[] tmps = tmp.split(",");
                uniqueSum += Long.parseLong(tmps[0]);
                allSum += Long.parseLong(tmps[1]);
            }

            collector.collect(new Text(key), new Text(uniqueSum + "," + allSum));
        }
    }

    public int run(String[] args) throws Exception {

        String inputPath = args[0];
        String outputPath = args[1];

        JobConf conf = new JobConf(getClass());
        //  conf.set("logDay", logDay);
        conf.setJobName("MRStatics-1-zhaoguoxian");
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(InfoMapper.class);
        // conf.setReducerClass(VCReducer.class);
        conf.setReducerClass(CountReducer.class);
        conf.setNumReduceTasks(10);

        // System.out.println("List log files " + logPath);
//        FileInputFormat.setInputPaths(conf, "s3://com.hasoffer.data/rawlog/20160530");
//        FileOutputFormat.setOutputPath(conf, new Path("s3://com.hasoffer.output/output1/20160601"));

        FileInputFormat.setInputPaths(conf, inputPath);
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
         
        JobClient.runJob(conf);

        JobConf countConf = new JobConf(getClass());
        countConf.setJobName("MRStatics-2-zhaoguoxian");
        countConf.setMapperClass(CountMapper1.class);
        countConf.setMapOutputKeyClass(Text.class);
        countConf.setMapOutputValueClass(Text.class);
        countConf.setInputFormat(TextInputFormat.class);
        countConf.setOutputFormat(TextOutputFormat.class);
        countConf.setOutputKeyClass(Text.class);
        countConf.setOutputValueClass(Text.class);
        countConf.setCombinerClass(CountReducer1.class);
        countConf.setReducerClass(CountReducer1.class);
        countConf.setNumReduceTasks(10);

        FileInputFormat.setInputPaths(countConf, outputPath);
        FileOutputFormat.setOutputPath(countConf, new Path(outputPath + "-2"));

        JobClient.runJob(countConf);
        return 0;
    }

    /**
     * @param args
     * @throws Exception
     */

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out
                    .println("Usage: HMTCheckYPStat need 2 arags args[0]:logPath, args[1]:outputPath");
            return;
        }

        int exitCode = ToolRunner.run(new MRStatics(), args);
        System.exit(exitCode);
    }

}
