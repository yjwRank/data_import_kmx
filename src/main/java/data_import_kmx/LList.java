package data_import_kmx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class LList implements Writable{
	private List title;
	public LList()
	{
		title=new LinkedList();
	}
	
	public void add(int num)
	{
		title.add(num);
	}
	
	public void clear()
	{
		title.clear();
	}
	
	public int get(int index)
	{
		return (Integer) title.get(index);
	}
	
	public int size()
	{
		return title.size();
	}
	
	public void readFields(DataInput in) throws IOException
	{
		title.clear();
		int count=in.readInt();
		for(int i=0;i<count;i++)
		{
			title.add(in.readInt());
		}
	}
	
	public void write(DataOutput out) throws IOException
	{
		out.writeInt(title.size());
		
		for(int i=0;i<title.size();i++)
		{
			out.writeInt((Integer) title.get(i));
			
		}
	}
}