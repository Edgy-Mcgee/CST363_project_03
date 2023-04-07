package heapdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class LSMmemoryTest {
	
	static LSMmemory lsm; 
	
	@BeforeEach
	public void init() {
		Schema sch = new Schema();
		sch.addKeyIntType("ID");
		sch.addVarCharType("name");
		lsm = new LSMmemory("lsmmemorytest", sch);
		for (int i=10000; i<10050; i++) {
			Tuple t = new Tuple(sch, i, "name:"+i);
			lsm.insert(t);
		}

	}
	
	@AfterEach
	public void cleanup() {
		if (lsm!=null) lsm.close();
	}

	// Test lookup found
	@Test
	//@Disabled
	public void lookup() {
		for (int i=10000; i<10050; i++) {
			Tuple t = lsm.lookup(i);
			assertNotNull(t);
			assertEquals(i, t.getKey());
			assertEquals("name:"+i, t.getString(1));
		}
	}
	
	// test delete, lookup not found
	@Test
	//@Disabled
	public void delete() {
		boolean r = lsm.delete(10001);
		assertTrue(r);
		Tuple t = lsm.lookup(10001);
		assertNull(t);
	}
	
	// test delete 
	@Test
	//@Disabled
	public void deleteAll() {
		for (int i=10000; i<10050; i++) {
			boolean r = lsm.delete(i);
			assertTrue(r);
			Tuple t = lsm.lookup(i);
			assertNull(t);
		}	
	}
	
	/*
	 * test that insert stores a copy of the tuple.
	 */
	@Test
	public void copyOnInsertTest() {
		Tuple t = new Tuple(lsm.getSchema(), 100, "test name" );
		boolean rc = lsm.insert(t);
		assertTrue(rc);
		Tuple s = lsm.lookup(100);
		assertFalse(t==s, "Insert did not copy tuple before put.");
	}
	
	/*
	 * test that lookup returns a copy of the tuple.
	 */
	@Test
	public void copyOnLookupTest() {
		Tuple t = lsm.lookup(10000);
		assertNotNull(t);
		t.set(0, 1);
		Tuple s = lsm.lookup(10000);
		assertFalse(s.getInt(0)==1, "lookup must return a copy of the tuple.");
	}
}
