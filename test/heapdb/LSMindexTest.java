package heapdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import java.util.Random;

public class LSMindexTest {
	
	/*
	 * test append and binary search of lookup in LSMindex
	 */
	@Test
	public void testIndex() {
		int numRecords = 500;
		int numLookups = 500;
		int blockno;
		
		Random gen = new Random();
		
		// create index entries 
		
		LSMindex index = new LSMindex();
		// append index entries
		for (int i=1; i<=numRecords; i++) {
			index.append(i, i+1);
		}
		
		// check for lookup of key less than lowest key in index
		blockno = index.lookup(0);
		assertEquals(Float.parseFloat("-1 should be returned for lookup with key less than lowest key in index"), -1, blockno);
		// check for lookup of key greater than highest key in index
		blockno = index.lookup(numRecords+1);
		assertEquals(Float.parseFloat("lookup of key greater than highest key in index should return last index entry"), numRecords+1, blockno);
		
		// now do random lookups 
		
		for (int i=0; i<numLookups; i++) {
			int key = gen.nextInt(numRecords)+1;
			blockno = index.lookup(key);
			assertEquals(key+1, blockno);
		}
	}

	/*
	 * test append and binary search of lookup in LSMindex for key of String datatype.
	 */
	@Test
	public void testIndexString() {
		int numRecords = 500;
		int numLookups = 500;
		int blockno;
		
		Random gen = new Random();
		
		// create index entries 
		
		LSMindex index = new LSMindex();
		// append index entries
		for (int i=1; i<=numRecords; i++) {
			String key = String.format("KEY%05d", i);
			index.append(key, i);
		}
		
		// check for lookup of key less than lowest key in index
		blockno = index.lookup("KEY0");
		assertEquals(Float.parseFloat("-1 should be returned for lookup with key less than lowest key in index"), -1, blockno);
		// check for lookup of key greater than highest key in index
		blockno = index.lookup("KEYA");
		assertEquals(Float.parseFloat("lookup of key greater than highest key in index should return last index entry"), numRecords, blockno);
		
		// now do random lookups 
		
		for (int i=0; i<numLookups; i++) {
			int k = gen.nextInt(numRecords)+1;
			String key = String.format("KEY%05d", k);
			blockno = index.lookup(key);
			assertEquals(k, blockno);
		}
	}

}
