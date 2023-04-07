package heapdb;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;

import org.junit.jupiter.api.Test;

public class LSMindexPerformance {
	
	@Test
	public void lookupTime() {
		
		/*
		 * create table with data
		 * compare time to do lookups without index to 
		 * lookups using index
		 */
		
		int numRecords = 10000;
		int numLookups = 5000;
		long time0, time1;

		Schema s = new Schema();
		s.addKeyIntType("ID");
		s.addVarCharType("name" );
		s.addVarCharType("dept_name" );
		s.addIntType("salary");
		
		LSMmemory table = new LSMmemory("speed", s);

		String[] depts = {"Biology", "Chemistry", "Physics", "Comp. Sci.", "Mathematics", "Statistics", "Pschology", "Spanish", "Latin", "Greek", 
				"English", "Japanese", "Chinese", "Communication", "Cinematography", "Art"};
		Tuple t = new Tuple(s);
		Random gen = new Random(123789);

		for (int rkey=1; rkey <= numRecords; rkey++) {
			t.set(0, rkey);
			t.set(1, "Name"+rkey);
			t.set(2, depts[gen.nextInt(depts.length)]);  // pick random department
			t.set(3, gen.nextInt(50000)+50000);  // random salary
			boolean result = table.insert(t);
			assertTrue(result);
		}
		
		LSMdisk.USE_INDEX = true;
		time0 = System.currentTimeMillis();
		
		for (int i=1; i<=numLookups; i++) {
			int key = gen.nextInt(numRecords)+1;
			Tuple r = table.lookup(key);
			assertNotNull(r);
		}
		
		time1 = System.currentTimeMillis();
		
		System.out.printf("elapsed time for %d lookups table size %d using index %.2f secs. \n" , numLookups, numRecords, (time1-time0)/1000. );
		
		LSMdisk.USE_INDEX=false;
		
		time0 = System.currentTimeMillis();
		
		for (int i=1; i<=numLookups; i++) {
			int key = gen.nextInt(numRecords)+1;
			Tuple r = table.lookup(key);
			assertNotNull(r);
		}
		
		time1 = System.currentTimeMillis();
		
		System.out.printf("elapsed time for %d lookups table size %d no index %.2f secs. \n" , numLookups, numRecords, (time1-time0)/1000. );
	
	}

}
