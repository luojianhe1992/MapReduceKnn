import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.niubility.data.ARFFInputformat;
import org.niubility.data.SparseVector;
import org.niubility.data.Vector2;
import org.niubility.data.Vector2SF;

public class KNNMapper extends Mapper<Text, SparseVector, Text, Vector2SF> {

	//variable test is to store the testing data
    private Vector<Vector2<String, SparseVector>> test =
            new Vector<Vector2<String, SparseVector>>();

    
    protected void map(
    		
    		//there are three parameters
            
    		Text key,
            
            //the SparseVector value is to store the one training data, each value contains 4 features
            SparseVector value,
            org.apache.hadoop.mapreduce.Mapper<Text, SparseVector, Text, Vector2SF>.Context context)
            throws java.io.IOException, InterruptedException {
    	
        // calculate the distance for each test sample with the training
    	
    	System.out.println("invoke the function map");
    	
        context.setStatus(key.toString());
        
        
        System.out.println("********************************");
        System.out.println("value in mapper is:"+value);
        System.out.println("value in mapper size is:"+value.size());
        System.out.println("test in mapper size is:"+test.size());
        System.out.println(value);
        System.out.println("********************************");
        
        
        for (Vector2<String, SparseVector> testCase : test) {
        	
        	/*
        	//calculate the dotProduct distance
            double d = testCase.getV2().dotProduct(value);
            */
        	
            //calculate the euclidean distance
        	double d = testCase.getV2().euclideanDistance(value);
            
            System.out.println("***************************");
            System.out.println("testCase in mapper is:"+testCase);
            
            System.out.println("value in mapper is:" + value);
            
            System.out.println("the distance d is:" + d);
            System.out.println("***************************");
            //after calculating the distance, transport the data to the next step
            context.write(new Text(testCase.getV1()), new Vector2SF(key.toString(),
                    (float) d));
        }

    }

    protected void cleanup(
            org.apache.hadoop.mapreduce.Mapper<Text, SparseVector, Text, Vector2SF>.Context context)
            throws java.io.IOException, InterruptedException {
//        test.close();
    }
    
    
    //set up the testing data
    protected void setup(
            org.apache.hadoop.mapreduce.Mapper<Text, SparseVector, Text, Vector2SF>.Context context)
            throws java.io.IOException, InterruptedException {
        System.out.print("loading shared comparison vectors...");

        // load the test vectors
        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(context.getConfiguration().get(
                "org.niubility.learning.test", "test.arff")))));
        String line = br.readLine();
        int count = 0;
        
        
        while (line != null) {
            Vector2<String, SparseVector> v = ARFFInputformat.readLine(count, line);
            test.add(new Vector2<String, SparseVector>(v.getV1(), v.getV2()));
            line = br.readLine();
            count++;
            
            //line是testing data
            /*
            System.out.println();
            System.out.println("*****************");
            System.out.println(line);
            System.out.println("*****************");
            System.out.println(count);
            System.out.println("*****************");
            */
        }
        br.close();
        System.out.println("done.");
    }
}
