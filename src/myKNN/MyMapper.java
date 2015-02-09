package myKNN;
import java.util.ArrayList;
import java.util.HashMap;


public class MyMapper {
	
	HashMap<Integer, Double> distanceToTrainingData;
	KnnNode testNode;
	ArrayList<KnnNode> trainingData;
	
	public MyMapper() {
		super();
		distanceToTrainingData = null;
	}

	public MyMapper(KnnNode node) {
		super();
		this.testNode = node;
		distanceToTrainingData = null;
	}

	public MyMapper(HashMap<Integer, Double> distanceToTrainingData, KnnNode node) {
		super();
		this.distanceToTrainingData = distanceToTrainingData;
		this.testNode = node;
	}
	
	public HashMap<Integer, Double> getDistanceToTrainingData() {
		return distanceToTrainingData;
	}

	public void setDistanceToTrainingData(HashMap<Integer, Double> distanceToTrainingData) {
		this.distanceToTrainingData = distanceToTrainingData;
	}

	public KnnNode getNode() {
		return testNode;
	}

	public void setNode(KnnNode node) {
		this.testNode = node;
	}
	
	public KnnNode getTestNode() {
		return testNode;
	}

	public void setTestNode(KnnNode testNode) {
		this.testNode = testNode;
	}

	public ArrayList<KnnNode> getTrainingData() {
		return trainingData;
	}

	public void setTrainingData(ArrayList<KnnNode> trainingData) {
		this.trainingData = trainingData;
	}

	//calculate the distance between testing data node to all the nodes in training data
	public void mapDistance(){
		for(int i=0;i<trainingData.size();i++){
			trainingData.get(i).getNodeDistance(testNode);
		}
	}
	
	//setup the training data
	public void setUpTrainingData(){
		String inputFileTrainingDataPath = "iris_train.csv";
		trainingData = DataReader.parse(inputFileTrainingDataPath);
	}
	
}
