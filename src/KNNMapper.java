import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
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
    
    //my define
    private ArrayList<KnnNode> trainingData = new ArrayList<KnnNode>();
    
    protected void map(
    		
    		//there are three parameters
            
    		Text key,
            
            //the SparseVector value is to store the one training data, each value contains 4 features
            SparseVector value,
            org.apache.hadoop.mapreduce.Mapper<Text, SparseVector, Text, Vector2SF>.Context context)
            throws java.io.IOException, InterruptedException {
    	
    	//set up the training data
        String trainingDataName = "iris_train2.csv";
        
        trainingData = TrainingDataReader.parse(trainingDataName, 0, new int[]{0,1,2,3}, 4);

        System.out.println("****************************************");
        System.out.println("trainingData size:"+trainingData.size());
        System.out.println("****************************************");
    	
        // calculate the distance for each test sample with the training
    	
    	System.out.println("invoke the function map");
    	
        context.setStatus(key.toString());
        
        
        System.out.println("********************************");
        //the variable value in mapper is one testing data
        System.out.println("value in mapper is:"+value);
        System.out.println("value in mapper size is:"+value.size());
        
        //the variable test in mapper is all training data
        System.out.println("test in mapper size is:"+test.size());
        System.out.println("test in mapper is:"+test);
        System.out.println("********************************");
        
        int index = 0;
        
        double [] features = new double [4];
        
        StringBuilder sb = new StringBuilder();
        sb.append(value.toString());
        sb.deleteCharAt(0);
        sb.deleteCharAt(sb.length() - 1);
        System.out.println("sb = "+sb.toString());
        
        String newString = sb.toString();
        String [] line = newString.split(",");
        for(int i=0;i<line.length;i++){
        	features[i] = Double.parseDouble(line[i].split("=")[1]);
        }
        /*
        for(int i=0;i<features.length;i++){
        	System.out.print("feature:"+features[i]);
        }
        */
        
        KnnNode oneTestingData = new KnnNode();
        oneTestingData.setFeatures(features);
        
        //calculate the distance
        for(KnnNode node:trainingData){
        	node.getNodeDistance(oneTestingData);
        	
        	//the data transform to reducer
        	context.write(new Text(oneTestingData.toString()), new Vector2SF(node.toString(),
        			(float)node.getDist()));
        }
        
        
        /*
        for (Vector2<String, SparseVector> testCase : test) {
        	
        	
//        	//calculate the dotProduct distance
//            double d = testCase.getV2().dotProduct(value);
            
        	
            //calculate the euclidean distance
        	double d = testCase.getV2().euclideanDistance(value);
        	System.out.println("distance "+index + " d is:" + d);
        	
        	
//            System.out.println("***************************");
//            System.out.println("testCase in mapper is:"+testCase);
//            
//            System.out.println("value in mapper is:" + value);
//            
//            System.out.println("the distance d is:" + d);
//            System.out.println("***************************");
            
            
            //after calculating the distance, transport the data to the next step
            context.write(new Text(testCase.getV1()), new Vector2SF(trainingData.get(index).toString(),
                    (float) d));
            
            index++;
        }
        */
        
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
