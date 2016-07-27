package DataImportKmx;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Zip {
	public Zip() {
		zipFileName = null;
		outputDirectory = null;
	}

	public void setZipFileName(String zip) {
		zipFileName = zip;
	}

	public void setOutputDirectory(String output) {
		outputDirectory = output;
	}

	public void unzip() {
		try {
			ZipInputStream in = new ZipInputStream(new FileInputStream(zipFileName));

			ZipEntry z = in.getNextEntry();
			while (z != null) {
				// System.out.println("unziping " + z.getName());

				File f = new File(outputDirectory);
				f.mkdir();
				if (z.isDirectory()) {
					String name = z.getName();
					name = name.substring(0, name.length() - 1);
					System.out.println("name " + name);
					f = new File(outputDirectory + File.separator + name);
					f.mkdir();
					System.out.println("mkdir " + outputDirectory + File.separator + name);
				} else {
					f = new File(outputDirectory + File.separator
							+ z.getName().substring(z.getName().lastIndexOf('\\') + 1, z.getName().length()));
					f.createNewFile();
					FileOutputStream out = new FileOutputStream(f);
					int b;
					while ((b = in.read()) != -1) {
						out.write(b);
					}
					out.close();
				}

				z = in.getNextEntry();
			}
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private String zipFileName;
	private String outputDirectory;
}
