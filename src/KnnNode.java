

public class KnnNode {
	String label;
	double [] features;
	double dist;
	
	public KnnNode() {
		super();
	}

	public KnnNode(double[] features) {
		super();
		this.features = features;
	}

	public KnnNode(String label, double[] features) {
		super();
		this.label = label;
		this.features = features;
	}
	
	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public double[] getFeatures() {
		return features;
	}

	public void setFeatures(double[] features) {
		this.features = features;
	}
	
	public double getDist() {
		return dist;
	}

	public void setDist(double dist) {
		this.dist = dist;
	}

	//get distance between two nodes
	public double getNodeDistance(KnnNode node){
		double distance = 0;
		
		for(int i = 0;i < features.length; i++){
			distance = distance + Math.pow(this.getFeatures()[i] - node.getFeatures()[i], 2);
		}
		
		this.dist = Math.sqrt(distance);
		return dist;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
		sb.append("label:").append(label).append(" features:");
		for(int i=0;i<features.length;i++){
			sb.append(features[i]+",");
		}
		sb.append("dist:"+dist);
		sb.append("\n");
		return sb.toString();
	}
	
}
