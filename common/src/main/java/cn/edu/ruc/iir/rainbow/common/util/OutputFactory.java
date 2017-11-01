package cn.edu.ruc.iir.rainbow.common.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class OutputFactory
{
	private static OutputFactory instance = null;
	private OutputFactory() {}
	
	public static OutputFactory Instance ()
	{
		if (instance == null)
		{
			instance = new OutputFactory();
		}
		return instance;
	}
	
	public BufferedWriter getWriter (String path) throws IOException
	{
		return new BufferedWriter(new FileWriter(path));
	}
}
