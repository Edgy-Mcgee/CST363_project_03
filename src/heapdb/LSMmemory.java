package heapdb;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class LSMmemory implements ITable {

	private Schema schema;
	private TreeMap<Object, Tuple> level0; // Object is the key:  String or Integer
	private LSMdisk disk;

	/**
	 * create a new LSM table
	 */
	public LSMmemory(String filename, Schema schema) {
		this.schema = schema;
		level0 = new TreeMap<>();
		disk = new LSMdisk(filename, schema);
	}

	/**
	 * open existing LSMdisk file. Schema is stored in block 0.
	 */

	public LSMmemory(String filename) {
		this.level0 = new TreeMap<>();
		disk = new LSMdisk(filename);
		this.schema = disk.getSchema();
	}

	@Override
	public void close() {
		disk.merge(this);
		disk.close();
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public int size() {
		throw new UnsupportedOperationException("LSM Table does not support size().");
	}

	@Override
	public boolean insert(Tuple rec) {
		if (level0.containsValue(rec)) { //Check if tuple is already in the tree
			return false;
		} else { //If not in tree...
			level0.put(rec.get(schema.getKey()), new Tuple(rec)); //...insert a copy of rec
			if (level0.size() > heapdb.Constants.LIMIT_0) {
				disk.merge(this);
				level0.clear();
			} // JUST ADDED
			return true;
		}
	}

	@Override
	public boolean delete(Object key) {
		if ( lookup(key) == null ) {
			return false;  // cannot delete key that does not exist
		}
		level0.put(key, new TupleDeleted(schema, key));
		return true;  // tuple marked for deletion
		// JUST ADDED
	}

	@Override
	public Tuple lookup(Object key) {
		Tuple t = level0.get(key);
		if (t!=null) {
			// return copy of t unless it is a TupleDeleted in which case return null.
			return (t instanceof TupleDeleted) ? null : new Tuple(t);
		} else {
			return disk.lookup(key);
		}
	}





	@Override
	public ITable lookup(String colname, Object value) {
		throw new UnsupportedOperationException("LSMmemory lookup(colname, value) not supported.");
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (Tuple t : this) {
			sb.append(t.toString());
			sb.append("\n");
		}
		return sb.toString();
	}

	@Override
	public Iterator<Tuple> iterator() {
		return new LSMmemoryIterator();
	}

	public class LSMmemoryIterator implements Iterator<Tuple> {
		Iterator<Map.Entry<Object, Tuple>> it0 = level0.entrySet().iterator();

		@Override
		public boolean hasNext() {
			return it0.hasNext();
		}

		@Override
		public Tuple next() {
			return it0.next().getValue();
		}
	}
}
