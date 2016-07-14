package data_import_kmx;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
 
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
 
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
   
    public  String unTargzFile(String rarFileName, String destDir) {
       GZip gzip = new GZip(rarFileName);
       String outputDirectory = destDir;
       File file = new File(outputDirectory);
       if (!file.exists()) {
           file.mkdir();
       }
       
       System.out.println("fgzip:"+file.toString());
       return gzip.unzipOarFile(outputDirectory);
    }
 
    public String unzipOarFile(String outputDirectory) {
       FileInputStream fis = null;
       ArchiveInputStream in = null;
       String outputPath=null;
       BufferedInputStream bufferedInputStream = null;
       try {
           fis = new FileInputStream(zipfileName);
           GZIPInputStream is = new GZIPInputStream(new BufferedInputStream(
                  fis));
           in = new ArchiveStreamFactory().createArchiveInputStream("tar", is);
           bufferedInputStream = new BufferedInputStream(in);
           TarArchiveEntry entry = (TarArchiveEntry) in.getNextEntry();
           while (entry != null) {
              String name = entry.getName();
              String[] names = name.split("/");
              String fileName = outputDirectory;
              for(int i = 0;i<names.length;i++){
                  String str = names[i];
                  fileName = fileName + File.separator + str;
              }
              if (name.endsWith("/")) {
                  mkFolder(fileName);
                  outputPath=fileName;
              } else {
                  File file = mkFile(fileName);
                  bufferedOutputStream = new BufferedOutputStream(
                         new FileOutputStream(file));
                  int b;
                  while ((b = bufferedInputStream.read()) != -1) {
                     bufferedOutputStream.write(b);
                  }
                  bufferedOutputStream.flush();
                  bufferedOutputStream.close();
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