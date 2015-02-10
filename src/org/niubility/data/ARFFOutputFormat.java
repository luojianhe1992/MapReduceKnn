package org.niubility.data;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * ARFF file hadoop output format
 * @author Song Liu (sl9885@bristol.ac.uk)
 */
public class ARFFOutputFormat extends TextOutputFormat<Text, SparseVector> {

    protected static class Writer extends RecordWriter<Text, SparseVector> {

        private LineRecordWriter<Text, Text> w;

        //constructor function
        public Writer(DataOutputStream dos, String codec) {
            w = new LineRecordWriter<Text, Text>(dos, codec);
        }

        public Writer(DataOutputStream dos) {
            w = new LineRecordWriter<Text, Text>(dos);
        }

        //各个class直接key和value之间的数据传递
        public synchronized void write(Text key, SparseVector value)
                throws IOException {
        	
            StringBuffer sb = new StringBuffer();

            SortedMap<String, Float> map = new TreeMap<String, Float>(value);
            
            System.out.println("*****************************");
        	System.out.println("variable key in the ARFFOutputFormat is:"+key);
        	System.out.println("variable value in the ARFFOutputFormat is:"+value);
        	System.out.println("variable map in the ARFFOutputFormat is:"+map);
        	System.out.println("*****************************");
            
            
            for (String col : map.keySet()) {
                sb.append(col + " " + map.get(col) + ",");
                System.out.println("variable col in the AFFOutputFormat is:"+col);
                System.out.println("map.get(col) in the AFFOutputFormat is:"+map.get(col));
            }
            
            StringBuilder sb2 = new StringBuilder();
            sb2.append(key.toString());
            sb2.delete(sb2.length() - 10, sb2.length() - 1);
            
            String []line =  sb2.toString().split("[ \\r\\n]+");
            
            System.out.println("*******************************");
            System.out.print("line 0 is:"+line[0]);
            System.out.print(" line 1 is:"+line[1]);
            System.out.println("*******************************");
            
            
            Text featureText = new Text(line[1]);
            Text labelText = new Text(line[0]);
            
            w.write(new Text(line[1]), new Text(line[0]));
            
            /*
            // remove the "," at the ending
            w.write(new Text("{" + SparseVector.ID + " " + key), new Text(sb.substring(0,
                    sb.length() - 1) + "}"));
            */
            
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException,
                InterruptedException {
            w.close(context);
        }
    }

    @Override
    public RecordWriter<Text, SparseVector> getRecordWriter(
            TaskAttemptContext job) throws IOException, InterruptedException {
        
    	Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        String keyValueSeparator = conf.get(
                "mapred.textoutputformat.separator", "\t");
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
                    job, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
                    conf);
            extension = codec.getDefaultExtension();
        }
        Path file = getDefaultWorkFile(job, extension);
        FileSystem fs = file.getFileSystem(conf);
        if (!isCompressed) {
            FSDataOutputStream fileOut = fs.create(file, false);
            return new Writer(fileOut, keyValueSeparator);
        } else {
            FSDataOutputStream fileOut = fs.create(file, false);
            return new Writer(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator);
        }
    }
}