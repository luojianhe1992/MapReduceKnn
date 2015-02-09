import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.niubility.data.ARFFInputformat;
import org.niubility.data.ARFFOutputFormat;
import org.niubility.data.SparseVector;
import org.niubility.data.Vector2SF;

public class KNNDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KNNDriver(), args);
        
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // config a job and start it
        Configuration conf = getConf();
        
        //对conf map，设置输出格式
        conf.set("mapred.textoutputformat.separator", ",");
        
        //look at the test folder
        for (FileStatus fs : FileSystem.get(conf).listStatus(new Path(args[2]))) {
            
        	System.out.println("*************************");
        	System.out.println("get path:"+fs.getPath());
        	System.out.println("*************************");
        	
        	//对conf map设置testing data的路径
        	conf.set("org.niubility.learning.test", fs.getPath().toString());
            
            @SuppressWarnings("deprecation")
			Job job = new Job(conf, "KNN Classifier");

            //asign the job
            job.setMapperClass(KNNMapper.class);
            job.setReducerClass(KNNReducer.class);
            job.setCombinerClass(KNNCombiner.class);
            job.setOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Vector2SF.class);
            job.setOutputValueClass(SparseVector.class);
            job.setInputFormatClass(ARFFInputformat.class);
            job.setOutputFormatClass(ARFFOutputFormat.class);
            
            //inputData path
            FileInputFormat.addInputPath(job, new Path(args[0]));
            
            
            //outputData path
            Path out = new Path(args[1]);
            FileSystem.get(conf).delete(out, true);
            FileOutputFormat.setOutputPath(job, out);
            //set the current testing file
            int res = job.waitForCompletion(true) ? 0 : 1;
            if (res != 0) {
                return res;
            }
        }
        
        System.out.println();
        
        return 0;
    }
}