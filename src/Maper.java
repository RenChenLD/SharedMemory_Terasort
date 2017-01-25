import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

class Maper implements Callable<File> {
	File file;
//	ArrayList<String> piece = new ArrayList<>();
	ArrayList<File> tmpFile = new ArrayList<>();
	long startPosition;
	long blockSize;
	int maperId, numOfTh;

	public Maper(File f, int maperId, int numOfThread, int bl) throws FileNotFoundException {
		this.file = f;
		this.blockSize = bl; // 100M
		this.maperId = maperId;
		this.numOfTh = numOfThread;
	}

	public void splite(File f) throws IOException {
		BufferedReader bufferedReader = new BufferedReader(new FileReader(f));
		String line;
		File output;
		BufferedWriter pWriter;
		TreeMap<String, String> piece = new TreeMap<>();
		int c = 0;
		for(int k=0; k<maperId; k++)
			bufferedReader.readLine();
		while ((line = bufferedReader.readLine()) != null) {
//			System.out.println(line.length());
			piece.put(line.substring(0, 10), line.substring(10));
//			piece.add(line);
			for (int i = 0; i < numOfTh - 1; i++)
				bufferedReader.readLine();
			if (piece.size() == blockSize) {
				output = new File(maperId +"tmp" + Integer.toString(c++));
				tmpFile.add(output);
				pWriter = new BufferedWriter(new FileWriter(output));
//				for (int j = 0; j < blockSize; j++) {
//					// System.out.println(piece.get(j));
//					pWriter.write(piece.get(j));
//					
//					pWriter.newLine();
//				}
				for(Map.Entry<String, String> entry : piece.entrySet())
				{
					pWriter.write(entry.getKey()+entry.getValue());
					pWriter.newLine();
				}
				pWriter.close();
				piece.clear();
			}
		}
		bufferedReader.close();
		
	}

	public void combiner() throws IOException {
		Reducer reducer;
		File file;
		int count = 0;
		while (tmpFile.size() != 1) {
			file = new File(maperId +"tmp" + count + "c");
			count++;
			reducer = new Reducer(tmpFile.get(0), tmpFile.get(1), file);
			reducer.reduce();
			tmpFile.get(1).delete();
			tmpFile.remove(1);
			tmpFile.get(0).delete();
			tmpFile.remove(0);
			tmpFile.add(file);
		}

	}

	public void map() throws IOException {
		System.out.println("Mapper" + maperId + " starts to split..");
		splite(file);
		System.out.println("Mapper" + maperId + " starts to combine..");
		combiner();
		System.out.println("Mapper" + maperId + " done.");
	}

	@Override
	public File call() throws Exception {
		map();
		return tmpFile.get(0);

	}
}