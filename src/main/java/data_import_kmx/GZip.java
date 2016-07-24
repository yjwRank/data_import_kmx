package data_import_kmx;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * 解压tar.gz文件包
 *
 */
public class GZip {

	private BufferedOutputStream bufferedOutputStream;

	private String zipfileName = null;

	public GZip(String fileName) {
		this.zipfileName = fileName;
	}
	public void ZipToDB(String filename,String dir)
	{
		try {
	         final int BUFFER = 2048;
	         BufferedOutputStream dest = null;
	         FileInputStream fis = new FileInputStream(filename);
	         CheckedInputStream checksum = new 
	           CheckedInputStream(fis, new Adler32());
	         ZipInputStream zis = new 
	           ZipInputStream(new 
	             BufferedInputStream(checksum));
	         ZipEntry entry;
	         while((entry = zis.getNextEntry()) != null) {
	        		String tmp=entry.getName();
	    			String str=dir+"/"+tmp.substring(tmp.lastIndexOf("\\")+1,tmp.length());
	    			System.out.println("Str:"+str);
	            int count;
	            byte data[] = new byte[BUFFER];
	            // write the files to the disk
	            FileOutputStream fos = new 
	              FileOutputStream(str);
	            dest = new BufferedOutputStream(fos, 
	              BUFFER);
	            while ((count = zis.read(data, 0, 
	              BUFFER)) != -1) {
	               dest.write(data, 0, count);
	            }
	            dest.flush();
	            dest.close();
	         }
	         zis.close();
	      } catch(Exception e) {
	         e.printStackTrace();
	      }
	   }
	public String unTargzFile(String rarFileName, String destDir) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(zipfileName), conf);
		Path inputPath = new Path(zipfileName);
		GZip gzip = new GZip(rarFileName);

		String outputDirectory = destDir;

		File file = new File(outputDirectory);
		if (!file.exists()) {
			file.mkdir();
		}
		//System.out.println("Tar:" + file.toString());
		//System.out.println("fgzip:" + file.toString());
		return gzip.unzipOarFile(outputDirectory);
		/*
		 * CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		 * CompressionCodec codec = factory.getCodec(inputPath); if(codec ==
		 * null){ System.out.println("no codec found for " + zipfileName);
		 * System.exit(1); } else { System.out.println("dame++find"); } String
		 * outputUri=CompressionCodecFactory.removeSuffix(zipfileName,
		 * codec.getDefaultExtension());
		 * System.out.println("outputUri:"+outputUri);
		 */
	}

	public String unzipOarFile(String outputDirectory) {
		FileInputStream fis = null;
		ArchiveInputStream in = null;
		String outputPath = null;
		BufferedInputStream bufferedInputStream = null;
		try {
			fis = new FileInputStream(zipfileName);
			GZIPInputStream is = new GZIPInputStream(new BufferedInputStream(fis));
			in = new ArchiveStreamFactory().createArchiveInputStream("tar", is);
			bufferedInputStream = new BufferedInputStream(in);
			TarArchiveEntry entry = (TarArchiveEntry) in.getNextEntry();
			while (entry != null) {
				String name = entry.getName();
				String[] names = name.split("/");
				String fileName = outputDirectory;
				for (int i = 0; i < names.length; i++) {
					String str = names[i];
					fileName = fileName + File.separator + str;
				}
				if (name.endsWith("/")) {
					mkFolder(fileName);
					outputPath = fileName;
				} else {
					File file = mkFile(fileName);
					bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file));
					int b;
					while ((b = bufferedInputStream.read()) != -1) {
						bufferedOutputStream.write(b);
					}
					bufferedOutputStream.flush();
					bufferedOutputStream.close();
					ZipToDB(fileName,outputPath);
					file.delete();
				}
				entry = (TarArchiveEntry) in.getNextEntry();
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ArchiveException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bufferedInputStream != null) {
					bufferedInputStream.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return outputPath;
	}

	private void mkFolder(String fileName) {
		File f = new File(fileName);
		if (!f.exists()) {
			f.mkdir();
		}
	}

	private File mkFile(String fileName) {
		File f = new File(fileName);
		try {
			f.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return f;
	}
}