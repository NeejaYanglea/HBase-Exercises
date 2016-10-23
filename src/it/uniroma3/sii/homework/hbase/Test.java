package it.uniroma3.sii.homework.hbase;

import java.io.IOException;

public class Test {
	
	public static void main(String[] argv) throws IOException {
		HBaseWrapper hw = new HBaseWrapper();
		
		try {
			System.out.println("Try: add record");
			hw.addRecord("blogposts", "post2", "post", "author", "Veronica");
			System.out.println("Try: get one record");
			RowBean row = hw.getOneRecord("blogposts", "post2");
			System.out.println(row.toString());
			System.out.println("Try: del record");
			hw.delRecord("blogposts", "post2");			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
