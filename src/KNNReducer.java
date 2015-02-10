import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.niubility.data.SparseVector;
import org.niubility.data.Vector2SF;

public class KNNReducer extends Reducer<Text, Vector2SF, Text, SparseVector> {

    protected void reduce(
    		//there are three parameters
            Text key,
            java.lang.Iterable<Vector2SF> value,
            org.apache.hadoop.mapreduce.Reducer<Text, Vector2SF, Text, SparseVector>.Context context)
            throws java.io.IOException, InterruptedException {
    	
    	//define an ArrayList to store the result
        ArrayList<Vector2SF> vs = new ArrayList<Vector2SF>();
        // sort each vector2SF by similarty
        
        
        
        System.out.println("************************");
        System.out.println("the function in the reducer");
        //the variable key contains the one testing data
        System.out.println("variable key in the reducer:" + key);
        
        //the variable value contains all training data and its distance with the testing data
        System.out.println("variable value in the reducer:"+value);
        System.out.println("************************");
        
        //the variable value is the result from the mapper, which means the distance between one testing node and all training nodes
        for (Vector2SF v : value) {
        	//getV1() means get the label, getV2 means get the distance
            vs.add(new Vector2SF(v.getV1(), v.getV2()));
        }
        
        /*
        System.out.println("**************************");
        System.out.println("testing to printout the lable "+vs.get(0).getV1().split(" ")[0].split(":")[1]);
        System.out.println("**************************");
        */
        
        
        System.out.println("**************************");
        
        //the variable vs contains all the information about the training data
        System.out.println("vs in reducer is:"+vs);
        System.out.println("vs in reducer size is:"+vs.size());
        System.out.println("**************************");
        
        //sort the distance result from mapper, from smaller to bigger
        Collections.sort(vs, new Comparator<Vector2SF>() {

            @Override
            public int compare(Vector2SF o1, Vector2SF o2) {
                return Float.compare(o1.getV2(), o2.getV2());
            }
        });
        
        
        System.out.println("**************************");
        
        //the variable vs contains all the information about the training data
        System.out.println("sorted vs in reducer is:"+vs);
        System.out.println("sorted vs in reducer size is:"+vs.size());
        System.out.println("**************************");
        
        
        //the knn default k is 4
        int k = context.getConfiguration().getInt("org.niubility.knn.k", 4);
        
        
        SparseVector sp = new SparseVector();
        
        String[] labelArray = new String[]{"setosa","versicolor","virginica"};
        int [] labelCount = new int [3];

        for (int i = 0; i < k && i < vs.size(); i++) {
        	if(vs.get(0).getV1().split(" ")[0].split(":")[1].equals(labelArray[0])){
        		labelCount[0]++;
        	}
        	if(vs.get(0).getV1().split(" ")[0].split(":")[1].equals(labelArray[1])){
        		labelCount[1]++;
        	}
        	if(vs.get(0).getV1().split(" ")[0].split(":")[1].equals(labelArray[2])){
        		labelCount[2]++;
        	}
        }
        int most = 0;
        int mostIndex = -1;
        for(int i=0;i<labelCount.length;i++){
        	if(labelCount[i] > most){
        		most = labelCount[i];
        		mostIndex = i;
        	}
        }
        
        String labelSettle = labelArray[mostIndex];
        
//        sp.put(vs.get(i).getV1(), vs.get(i).getV2());
        
        StringBuilder sb = new StringBuilder();
        sb.append(key.toString());
        sb.delete(6, 10);
        sb.insert(6, labelSettle);
        
        Text newKey = new Text(sb.toString());
        
        System.out.println("*****************************");
        System.out.println("newKey in the reducer is: "+newKey.toString());
        System.out.println("*****************************");
        
        //将每一个mapper的id号key，以及reducer处理后的结果sp，传递给下一个类
       
        sp.put(newKey.toString(), (float)labelCount[mostIndex]);
        
        context.write(newKey, sp);
        
    }
}