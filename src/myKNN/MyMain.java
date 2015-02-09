package myKNN;
import java.util.ArrayList;


public class MyMain {
	
	static int testingSize;
	
	public static void main(String[] args) {
		
		String inputFileTraingDataName = "iris_train.csv";
		String inputFileTestingDataName = "iris_test_data.csv";
		
		ArrayList<KnnNode> trainingData = DataReader.parse(inputFileTraingDataName);
		ArrayList<KnnNode> testingData = DataReader.parseTestingData(inputFileTestingDataName);
		
		final ArrayList<ArrayList<Double>> distanceResult = new ArrayList<ArrayList<Double>>();
		
		testingSize = testingData.size();
		
		for(final KnnNode testNode: testingData){
			new Thread(new Runnable() {
				@Override
				public void run() {
					// TODO Auto-generated method stub
					MyMapper myMapper = new MyMapper(testNode);
					myMapper.setUpTrainingData();
					myMapper.mapDistance();
					ArrayList<Double> tempDistanceResult = new ArrayList<Double>();
					for(int i=0;i<myMapper.getTrainingData().size();i++){
						tempDistanceResult.add(myMapper.getTrainingData().get(i).getDist());
					}
					distanceResult.add(tempDistanceResult);
				}
			}).start();
		}
		
		System.out.println("end");
		
		
		
	}
}
