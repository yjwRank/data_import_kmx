package data_import_kmx;

import java.io.File;
import java.util.LinkedList;

public class GoldwindToCSV {
				public void ZipToDB(String path) 
				{
					 int fileNum = 0, folderNum = 0;
				        File file = new File(path);
				        if (file.exists()) {
				            LinkedList<File> list = new LinkedList<File>();
				            File[] files = file.listFiles();
				            for (File file2 : files) {
				                if (file2.isDirectory()) {
				                    System.out.println("文件夹:" + file2.getAbsolutePath());
				                    list.add(file2);
				                    fileNum++;
				                } else {
				                    System.out.println("文件:" + file2.getAbsolutePath());
				                   Zip tmp=new Zip();
				                    tmp.setZipFileName(file2.getAbsolutePath());
				                    String s=file2.getAbsolutePath();
				                    tmp.setOutputDirectory(s.substring(0, s.lastIndexOf('/')));
			
				                    tmp.unzip();
				                    file2.delete();
				                    folderNum++;
				                }
				            }
				            File temp_file;
				            while (!list.isEmpty()) {
				                temp_file = list.removeFirst();
				                files = temp_file.listFiles();
				                for (File file2 : files) {
				                    if (file2.isDirectory()) {
				                        System.out.println("文件夹:" + file2.getAbsolutePath());
				                        list.add(file2);
				                        fileNum++;
				                    } else {
				                        System.out.println("文件:" + file2.getAbsolutePath());
				                        folderNum++;
				                    }
				                }
				            }
				        } else {
				            System.out.println("文件不存在!");
				        }
				        System.out.println("文件夹共有:" + folderNum + ",文件共有:" + fileNum);
				}
				public void TraversFolder(String path)
				{
					 int fileNum = 0, folderNum = 0;
				        File file = new File(path);
				        if (file.exists()) {
				            LinkedList<File> list = new LinkedList<File>();
				            File[] files = file.listFiles();
				            for (File file2 : files) {
				                if (file2.isDirectory()) {
				                    System.out.println("文件夹:" + file2.getAbsolutePath());
				                    list.add(file2);
				                    fileNum++;
				                } else {
				                    System.out.println("文件:" + file2.getAbsolutePath());
				                    GoldWindToZip(file2);
				                    folderNum++;
				                }
				            }
				            File temp_file;
				            while (!list.isEmpty()) {
				                temp_file = list.removeFirst();
				                files = temp_file.listFiles();
				                for (File file2 : files) {
				                    if (file2.isDirectory()) {
				                        System.out.println("文件夹:" + file2.getAbsolutePath());
				                        list.add(file2);
				                        fileNum++;
				                    } else {
				                        System.out.println("文件:" + file2.getAbsolutePath());
				                        folderNum++;
				                    }
				                }
				            }
				        } else {
				            System.out.println("文件不存在!");
				        }
				        System.out.println("文件夹共有:" + folderNum + ",文件共有:" + fileNum);

				}
				
				public boolean GoldWindToZip(File file)
				{
					
					String filename=file.getAbsolutePath();
				    if(filename.indexOf(".")>=0)
				    {
				    		filename = filename.substring(0, filename.lastIndexOf("."));
				    }
				    else
				    {
				    		System.out.println("err filename:"+filename+" can't change name");
				    		return false;
				    }
				    
				    if(file.renameTo(new File(filename+".zip")))
				    {
				    	  return true;
				    }
				    else
				    {
				    	System.out.println("filename:"+file+"  rename failed ");
				    	return false;
				    }
				
				}
}
