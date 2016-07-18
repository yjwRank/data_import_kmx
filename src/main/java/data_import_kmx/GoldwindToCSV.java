package data_import_kmx;

import java.io.File;
import java.util.LinkedList;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.math3.fitting.PolynomialFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoint;
public class GoldwindToCSV {
	
				public void DbToCSV(String path) throws ClassNotFoundException, IOException, SQLException
				{
					int fileNum = 0, folderNum = 0;
			        File file = new File(path);
			        Class.forName("org.sqlite.JDBC");
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
			                    String filename=file2.getAbsolutePath();
			                    String outputFile=filename.substring(0,filename.lastIndexOf('.'))+".csv";
			                    System.out.println("outputFile:"+outputFile);
			                    BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
			                    Connection conn = DriverManager.getConnection("jdbc:sqlite:"+filename);
			            		Statement stmt = conn.createStatement();
			            		ResultSet rs = stmt.executeQuery("select * from RUNDATA");
			            		ResultSetMetaData rsmd=rs.getMetaData();
			            		String TuiName=null;
			            		TuiName=filename.substring(filename.lastIndexOf('/')+1,filename.lastIndexOf('.'));
			            		TuiName=TuiName.substring(0, TuiName.length()-8);
			            		String name=null;
			            		int colNum = rs.getMetaData().getColumnCount();
			            		for(int i=1;i<=colNum;i++)
			            		{
			            			//System.out.println(rsmd.getColumnName(i));
			            			name=rsmd.getColumnName(i);
			            			bw.write(name+",");
			            		}
			            		bw.write("turbineID"+"\n");
			            		while(rs.next()) {
			            			for(int i = 1; i <= colNum; i++) {
			            				bw.write(rs.getString(i) + ",");
			            			}
			            			bw.write(TuiName + "\n");
			            		}
			            		conn.close();
			            		bw.flush();
			            		bw.close();
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
				                    System.out.println("s:"+s+"  name:"+s.substring(0, s.lastIndexOf('/')));
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
