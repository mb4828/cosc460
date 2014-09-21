package simpledb;
import java.io.*;

public class Lab2Main {

	private static String printtup(Tuple tup) {
		String out = "";
		for (int i=0; i<3; i++) {
			out += tup.getField(i).toString() + " ";
		}
		return out;
	}
	
    public static void main(String[] argv) {

        // construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("some_data_file.dat"), descriptor);
        Database.getCatalog().addTable(table1, "test");

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());
        
        Field testfield = (Field) new IntField(3);
        Predicate.Op testop = Predicate.Op.valueOf("LESS_THAN");
        
        try {
        	
        	f.open();
        	while (f.hasNext()) {
        		Tuple tup = f.next();
        		
        		// check if field1 < 3
        		if (tup.getField(1).compare(testop, testfield)) {
        			
        			// create a new tuple with updated values
        			Tuple newtup = new Tuple(descriptor);
        			newtup.setField(0, tup.getField(0));
        			newtup.setField(1, testfield);
        			newtup.setField(2, tup.getField(2));
        			
        			System.out.println("Update tuple: " + printtup(tup) + " to be: " + printtup(newtup));
        			
        			// update the old tuple with a new tuple
        			table1.deleteTuple(tid, tup);
        			table1.insertTuple(tid, newtup);
        		}
        	}
        	f.close();
        
	        // add tuple 9 9 9
	        Tuple newtup = new Tuple(descriptor);
	        Field newfield = (Field) new IntField(99);
	        
	        newtup.setField(0, newfield);
	        newtup.setField(1, newfield);
	        newtup.setField(2, newfield);
	        
	        System.out.println("Insert tuple: " + printtup(newtup));
	        
	        table1.insertTuple(tid, newtup);
	        
	        // print out the records in the table
	        System.out.println("The table now contains the following records:");
	        
	        	f.open();
	        	f.rewind();
	        	while (f.hasNext()) {
	        		Tuple tup = f.next();
	        		System.out.println("Tuple: " + printtup(tup));
	        	}
	        	f.close();
	        	
        } catch (Exception e) {
        	System.out.println("Exception : " + e);
        } 
        
    }

}
