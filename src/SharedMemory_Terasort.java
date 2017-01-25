import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SharedMemory_Terasort {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {

		Long start = System.currentTimeMillis();

		int num = Integer.parseInt(args[0]);
		File inputF = new File(args[1]);

		long size = inputF.length();
		System.out.println("File length: " + size);
		System.out.println("Num of threads: " + num);
		int blockSize = Integer.parseInt(args[2]);
//		File output = new File(args[2]);
//		if (!output.exists()) {
//			output.mkdirs();
//		} else
//			output.delete();
//
		ExecutorService poolm = Executors.newFixedThreadPool(num);

		Maper[] mapers = new Maper[num];
		Future<File>[] f = new Future[num];
		for (int t = 0; t < num; t++) {
			mapers[t] = new Maper(inputF, t, num, blockSize);
			f[t] = poolm.submit(mapers[t]);
		}
		ArrayList<File> finalFile = new ArrayList<>();
		for (int t = 0; t < num; t++)
			finalFile.add(f[t].get());
		
		Reducer reducer;
		File ffile;
		int count =0;
		System.out.println("Reducer starts...");
		while(finalFile.size()!= 1)
		{
			ffile = new File("final"+count);
			count ++;
			reducer = new Reducer(finalFile.get(0), finalFile.get(1), ffile);
			reducer.reduce();
			finalFile.get(1).delete();
			finalFile.remove(1);
			finalFile.get(0).delete();
			finalFile.remove(0);
			finalFile.add(ffile);
		}
		System.out.println("Reducer done.");
		
		poolm.shutdown();

		Long end = System.currentTimeMillis();
		float duration = (float) ((end - start) / 1000.00);
		System.out.println("Duration: " + duration + " s");
		System.out.println("Throughput: " + (float) (size / 1000000 / duration) + " MB/s");
		
		
		
		
	}
}