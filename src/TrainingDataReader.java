import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;



import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;


public class TrainingDataReader {
	//parse training data
		public static ArrayList<KnnNode> parse(String path) {
			return parse(path, 0, new int[] { 0, 1, 2, 3 }, 4);
		}
		
		//override function
		public static ArrayList<KnnNode> parse(String path, int skipline, int[] featureColumnIndex, int labelColumn) {
			
			//using a ArrayList<KnnNode> to store the training data
			ArrayList<KnnNode> result = new ArrayList<KnnNode>();

			//initialize the InputStream
			InputStream stream = TrainingDataReader.class.getClassLoader().getResourceAsStream(path);
			
			//define a CSVParser
			CSVParser parse;
			
			//
			List<CSVRecord> records = null;
			try {
				parse = CSVFormat.EXCEL.parse(new InputStreamReader(stream, "ISO-8859-1"));
				records = parse.getRecords();
			} catch (IOException e1) {
				e1.printStackTrace();
			}

			for (CSVRecord record : records) {
				if (record.getRecordNumber() <= skipline)
					continue;
				double[] features = new double[featureColumnIndex.length];

				for (int i = 0; i < featureColumnIndex.length; i++) {
					double feature = Double.parseDouble(record.get(featureColumnIndex[i]));
					features[i] = feature;
				}
				result.add(new KnnNode(record.get(labelColumn),features));
			}
			return result;
		}
		
		//parse testing data
		public static ArrayList<KnnNode> parseTestingData(String path){
			return parseTestingData(path, 0, new int []{0,1,2,3});
		}
		
		public static ArrayList<KnnNode> parseTestingData(String path, int skipline, int[]featureColumnIndex){
					
			//using a ArrayList<KnnNode> to store the training data
			ArrayList<KnnNode> result = new ArrayList<KnnNode>();

			//initialize the InputStream
			InputStream stream = TrainingDataReader.class.getClassLoader().getResourceAsStream(path);
					

			//define a CSVParser
			CSVParser parse;
					
			//
			List<CSVRecord> records = null;
			try {
				parse = CSVFormat.EXCEL.parse(new InputStreamReader(stream, "ISO-8859-1"));
				records = parse.getRecords();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			for (CSVRecord record : records) {
				if (record.getRecordNumber() <= skipline)
					continue;
				double[] features = new double[featureColumnIndex.length];
				for (int i = 0; i < featureColumnIndex.length; i++) {
					double feature = Double.parseDouble(record.get(featureColumnIndex[i]));
					features[i] = feature;
				}
				result.add(new KnnNode(features));
			}
			return result;
		}
}
