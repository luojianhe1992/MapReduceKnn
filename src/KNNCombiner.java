import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.niubility.data.Vector2SF;

public class KNNCombiner extends Reducer<Text, Vector2SF, Text, Vector2SF> {
        
	protected void reduce(
			//there are three parameters
			Text key,
			java.lang.Iterable<Vector2SF> value,
			org.apache.hadoop.mapreduce.Reducer<Text, Vector2SF, Text, Vector2SF>.Context context)
					throws java.io.IOException, InterruptedException {
        	
		//variable vs is the result from mapper, which contains the result of distance between each testing node and all training nodes
		ArrayList<Vector2SF> vs = new ArrayList<Vector2SF>();
        
		//add all the values which contain the distance between each testing node and training node to the variable vs
		for (Vector2SF v : value) {
			//getV1() is to get the label, getV2() is to get the distance
			vs.add(new Vector2SF(v.getV1(), v.getV2()));
		}
        
		System.out.println("************************");
		System.out.println("vs in combiner is:"+vs);
		System.out.println("vs in combiner size is:"+vs.size());
		System.out.println("************************");
		
		//sort the vs according to the distance from smaller to bigger
		Collections.sort(vs, new Comparator<Vector2SF>() {
			@Override
			public int compare(Vector2SF o1, Vector2SF o2) {
				return Float.compare(o1.getV2(), o2.getV2());
			}
		});
		
		System.out.println("************************");
		System.out.println("the sorted vs in combiner is:"+vs);
		System.out.println("************************");
		
		System.out.println("************************");
		System.out.print("vs in combiner getV1() is:");
		for(int i=0;i<vs.size();i++){
			System.out.print(vs.get(i).getV1());
		}
		System.out.println();
		System.out.println("************************");
		
		int k = context.getConfiguration().getInt("org.niubility.knn.k", 4);
		//pick the k least distance node
		for (int i = 0; i < k && i < vs.size(); i++) {
			context.write(key, vs.get(i));
		}
	}
}
