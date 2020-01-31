package cl.cam.ac.uk.tuneful.util;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Hashtable;

public class Util {
	public static Hashtable loadTable(String path) {
		Hashtable table = new Hashtable();
		try {
			FileInputStream fileIn = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			table = (Hashtable) in.readObject();
//		      System.out.println(h.toString( )); 
		} catch (Exception e) {
			System.out.println(e);		
		}
		return table;
	}
	
	public static void writeTable(Hashtable table, String path) {
		try {
			FileOutputStream fileOut = new FileOutputStream(path);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(table);
		} catch (Exception e) {
			System.out.println(e);
		}
	}
}
