package simpledb;

import java.io.IOException;

public class Lab3Main {

    public static void main(String[] argv) 
       throws DbException, TransactionAbortedException, IOException {

        System.out.println("Loading schema from file:");
        // file named college.schema must be in mysimpledb directory
        Database.getCatalog().loadSchema("college.schema");
        
	    /*
	     * 	SQL query: SELECT * FROM STUDENTS WHERE name="Alice"
	     * 	algebra translation: select_{name="alice"}( Students )
	     * 	query plan: a tree with the following structure
	     * 	- a Filter operator is the root; filter keeps only those w/ name=Alice
	     * 	- a SeqScan operator on Students at the child of root
	     */
	        
	    /*    TransactionId tid = new TransactionId();
	        SeqScan scanStudents = new SeqScan(tid, Database.getCatalog().getTableId("students"));
	        StringField alice = new StringField("alice", Type.STRING_LEN);
	        Predicate p = new Predicate(1, Predicate.Op.EQUALS, alice);
	        Filter filterStudents = new Filter(p, scanStudents);
	
	        // query execution: we open the iterator of the root and iterate through results
	        System.out.println("Query results:");
	        filterStudents.open();
	        while (filterStudents.hasNext()) {
	            Tuple tup = filterStudents.next();
	            System.out.println("\t"+tup);
	        }
	        filterStudents.close();
	        Database.getBufferPool().transactionComplete(tid); */
        
        /*
         *	SQL query: SELECT * FROM COURSES, PROFS WHERE cid = favoriteCourse
         *	algebra translation: join_{cid=favoriteCourse}( courses, profs )
         *	query plan: 
         *	- Join operator as root with SeqScan operators on courses, profs
         */
        
	    /*    TransactionId tid = new TransactionId();
	        SeqScan scanCourses = new SeqScan(tid, Database.getCatalog().getTableId("courses"));
	        SeqScan scanProfs = new SeqScan(tid, Database.getCatalog().getTableId("profs"));
	        JoinPredicate j = new JoinPredicate(2,Predicate.Op.EQUALS,0);
	        Join joincp = new Join(j, scanProfs, scanCourses);
	        
	        // query execution
	        System.out.println("Query results:");
	        joincp.open();
	        while (joincp.hasNext()) {
	        	Tuple tup = joincp.next();
	        	System.out.println("\t"+tup);
	        }
	        joincp.close();
	        Database.getBufferPool().transactionComplete(tid); */
        
        /*
         *	SQL query: SELECT * FROM STUDENTS S, TAKES T WHERE S.sid = T.sid
         *	algebra translation: join_{S.sid=T.sid}( students s, takes t )
         *	query plan: 
         *	- Join operator as root with SeqScan operators on students, takes
         */
        
        TransactionId tid = new TransactionId();
        SeqScan scanStudents = new SeqScan(tid, Database.getCatalog().getTableId("students"));
        SeqScan scanTakes = new SeqScan(tid, Database.getCatalog().getTableId("takes"));
        JoinPredicate j = new JoinPredicate(0,Predicate.Op.EQUALS,0);
        Join joinst = new Join(j, scanStudents, scanTakes);
        
        // query execution
        System.out.println("Query results:");
        joinst.open();
        while (joinst.hasNext()) {
        	Tuple tup = joinst.next();
        	System.out.println("\t"+tup);
        }
        joinst.close();
        Database.getBufferPool().transactionComplete(tid);
    }

}