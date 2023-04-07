package heapdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import java.util.Random;

public class LSMmemoryTestManyrows {
	
	@Test
	public void rows1000() {
		Schema s = new Schema();
		s.addKeyIntType("ID");
		s.addVarCharType("name" );
		s.addVarCharType("dept_name" );
		s.addIntType("salary");
		
		
		String[] depts = {"Biology", "Chemistry", "Physics", "Comp. Sci.", "Mathematics", "Statistics", "Pschology", "Spanish", "Latin", "Greek", 
				"English", "Japanese", "Chinese", "Communication", "Cinematography", "Art"};
		Tuple t = new Tuple(s);
		Random gen = new Random(123789);
		int inserts = 0;
		int deletes = 0;
		
		LSMmemory table = new LSMmemory("rows1000", s);
		int[] keys = new int[1001];  // keys[k]==1 key k exists,  keys[k]==0  key k does not exist
		
		long time0 = System.currentTimeMillis();
		
		for (int i=0; i<10000; i++) {  // repeat 10000 times
			int rkey = gen.nextInt(1000)+1;  // generate random key in range [1, 1000]
			if (keys[rkey]==0) {
				// rkey does not exist, create tuple and insert it.
				t.set(0, rkey);
				t.set(1, "Name"+rkey);
				t.set(2, depts[gen.nextInt(depts.length)]);  // pick random department
				t.set(3, gen.nextInt(50000)+50000);  // random salary
				boolean result = table.insert(t);
				assertTrue(result);
				keys[rkey]=1;
				inserts++;
			} else {
				// rkey should exist.  Find it and delete it.
				Tuple t1 = table.lookup(rkey);
				assertNotNull(t1);
				assertEquals(rkey, t1.getInt(0));
				boolean result = table.delete(rkey);
				assertTrue(result);
				keys[rkey]=0;
				deletes++;
			}
		}
		
		long time1 = System.currentTimeMillis();
		
		System.out.printf("inserts = %d, deletes=%d, elapsed time=%.2f secs\n", inserts, deletes, (time1-time0)/1000.0);
	
		for (int i=0; i<keys.length; i++) {
			if (keys[i]!=0) {
				Tuple t2 = table.lookup(i);
				assertNotNull(t2);
				assertEquals(i, t2.getInt(0));
			} else {
				Tuple t3 = table.lookup(i);
				assertNull(t3);
			}
		}
	}

}
