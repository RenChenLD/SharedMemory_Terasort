import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class Reducer {
	File f1;
	File f2;
	File output;

	public Reducer(File f1, File f2, File output) {
		this.f1 = f1;
		this.f2 = f2;
		this.output = output;
	}

	@SuppressWarnings("resource")
	public void reduce() throws IOException {
		String line1, line2;
		BufferedReader br1 = new BufferedReader(new FileReader(f1));
		BufferedReader br2 = new BufferedReader(new FileReader(f2));
		line1 = br1.readLine();
		line2 = br2.readLine();

		BufferedWriter pWriter = new BufferedWriter(new FileWriter(output));

		while (line1 != null && line2 != null) {
			if (line1.compareTo(line2) < 0) {
				pWriter.write(line1);
				pWriter.newLine();
				line1 = br1.readLine();
			} else {
				pWriter.write(line2);
				pWriter.newLine();
				line2 = br2.readLine();
			}
		}
		if (line1 == null && line2 != null)
			while (line2 != null) {
				pWriter.write(line2);
				pWriter.newLine();
				line2 = br2.readLine();
			}
		else if (line1 != null && line2 == null) {
			while (line1 != null) {
				pWriter.write(line1);
				pWriter.newLine();
				line1 = br1.readLine();
			}
		}
		pWriter.close();
	}
}