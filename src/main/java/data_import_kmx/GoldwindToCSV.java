package data_import_kmx;

import java.io.File;
import java.util.LinkedList;
import java.util.Queue;
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

	public void DbToCSV(String path) throws ClassNotFoundException, IOException, SQLException {
		int fileNum = 0, folderNum = 0;
		File file = new File(path);
		Class.forName("org.sqlite.JDBC");
		Queue<File> list = new LinkedList<File>();
		if (file.exists()) {
			File[] files = file.listFiles();
			for (File file2 : files) {
				list.add(file2);
			}
			while (list.size() > 0) {
				File tmp = list.poll();
				if (tmp.isDirectory()) {
					System.out.println("文件夹：" + tmp.getAbsolutePath());
					File[] files2 = tmp.listFiles();
					for (File file3 : files2) {
						list.add(file3);
					}
				} else {
					System.out.println("文件" + tmp.getAbsolutePath());
					String filename = tmp.getAbsolutePath();
					String outputFile = filename.substring(0, filename.lastIndexOf('.')) + ".csv";
					System.out.println("outputFile:" + outputFile);
					BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
					Connection conn = DriverManager.getConnection("jdbc:sqlite:" + filename);
					Statement stmt = conn.createStatement();
					ResultSet rs = stmt.executeQuery("select * from RUNDATA");
					ResultSetMetaData rsmd = rs.getMetaData();
					String TuiName = null;
					TuiName = filename.substring(filename.lastIndexOf('/') + 1, filename.lastIndexOf('.'));
					TuiName = TuiName.substring(0, TuiName.length() - 8);
					String name = null;
					int colNum = rs.getMetaData().getColumnCount();
					for (int i = 1; i <= colNum; i++) {
						// System.out.println(rsmd.getColumnName(i));
						name = rsmd.getColumnName(i);
						bw.write(name + ",");
					}
					bw.write("turbineID" + "\n");
					while (rs.next()) {
						for (int i = 1; i <= colNum; i++) {
							bw.write(rs.getString(i) + ",");
						}
						bw.write(TuiName + "\n");
					}
					conn.close();
					bw.flush();
					bw.close();
					tmp.delete();
				}
			}

		} else {
			System.out.println("no file or directory");
		}
	}

	public void ZipToDB(String path) {
		int fileNum = 0, folderNum = 0;
		File file = new File(path);
		Queue<File> list = new LinkedList<File>();
		if (file.exists()) {
			File[] files = file.listFiles();
			for (File file2 : files) {
				list.add(file2);
			}
			while (list.size() > 0) {
				File tmp = list.poll();
				if (tmp.isDirectory()) {
					System.out.println("文件夹：" + tmp.getAbsolutePath());
					File[] files2 = tmp.listFiles();
					for (File file3 : files2) {
						list.add(file3);
					}
				} else {
					System.out.println("文件：" + tmp.getAbsolutePath());
					Zip zip = new Zip();
					String s = tmp.getAbsolutePath();
					zip.setZipFileName(s);
					zip.setOutputDirectory(s.substring(0, s.lastIndexOf('/')));
					//System.out.println("s:" + s + "  name:" + s.substring(0, s.lastIndexOf('/')));
					zip.unzip();
					tmp.delete();
				}
			}
		} else {
			System.out.println("file not exist");
		}

	}

	public void TraversFolder(String path) {
		int fileNum = 0, folderNum = 0;
		File file = new File(path);
		Queue<File> list = new LinkedList<File>();
		if (file.exists()) {
			File[] files = file.listFiles();
			for (File file2 : files) {
				list.add(file2);
			}
			while (list.size() > 0) {
				File tmp = list.poll();
				if (tmp.isDirectory()) {
					System.out.println("文件夹："+tmp.getAbsolutePath());
					File[] files2=tmp.listFiles();
					for(File file3:files2)
					{
						list.add(file3);
					}
				}else{
					System.out.println("文件："+tmp.getAbsolutePath());
					GoldWindToZip(tmp);
				}
			}
		} else {
			System.out.println("file not exist");
		}

	}

	public boolean GoldWindToZip(File file) {

		String filename = file.getAbsolutePath();
		if (filename.indexOf(".") >= 0) {
			filename = filename.substring(0, filename.lastIndexOf("."));
		} else {
			System.out.println("err filename:" + filename + " can't change name");
			return false;
		}

		if (file.renameTo(new File(filename + ".zip"))) {
			return true;
		} else {
			System.out.println("filename:" + file + "  rename failed ");
			return false;
		}

	}
}
