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
        
        //the variable value is the result from the mapper, which means the distance between one testing node and all training nodes
        for (Vector2SF v : value) {
        	//getV1() means get the label, getV2 means get the distance
            vs.add(new Vector2SF(v.getV1(), v.getV2()));
        }
        
        System.out.println("**************************");
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
        
        //the knn default k is 4
        int k = context.getConfiguration().getInt("org.niubility.knn.k", 4);
        
        
        SparseVector sp = new SparseVector();

        for (int i = 0; i < k && i < vs.size(); i++) {
            sp.put(vs.get(i).getV1(), vs.get(i).getV2());
        }
        
        //将每一个mapper的id号key，以及reducer处理后的结果sp，传递给下一个类
        context.write(key, sp);
    }
}