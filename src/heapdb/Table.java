package heapdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Table implements ITable {
	
	private List<Tuple> tuples;
	private Schema schema;
	
	public Table(Schema schema) {
		this.schema = schema;
		tuples = new ArrayList<>();
	}
	
	@Override
	public Schema getSchema() {
		return schema;
	}

	
	@Override
	public int size() {
		return tuples.size();
	}

	@Override
	public void close() {
		// do nothing
	}
	
	@Override
	public boolean insert(Tuple rec) {
		if (! rec.getSchema().equals(schema)) {
			throw new IllegalStateException("Error: tuple schema does not match table schema.");
		}

		if(schema.getKey() != null){
			for(Tuple t: tuples){
				if(t.getKey().equals(rec.getKey())){ //If the key matches another in the table
					return false;
				}
			}
		}

		tuples.add(new Tuple(rec)); //If the key is unique add it to the table
		return true;
		
	}

	@Override
	public boolean delete(Object key) {
		if (schema.getKey() == null) {
			throw new IllegalStateException("Error: table does not have a primary key.  Can not delete.");
		}

		for(Tuple t: tuples){ //Find the location of the key
			if(t.getKey().equals(key)){
				tuples.remove(key); //Remove the key once found
				return true; //Return true for successful removal
			}
		}
		return false; //Return false if not deleted
	}


	@Override
	public Tuple lookup(Object key) {
		if (schema.getKey() == null) {
			throw new IllegalStateException("Error: table does not have a primary key.  Can not lookup by key.");
		}
		Tuple copy = null;

		for(Tuple t: tuples){ //Find the location of the given key
			if (t.getKey().equals(key)){ //If found...
				copy = new Tuple(t);
			}
		}
		return copy; //Return the copy
	}

	@Override
	public ITable lookup(String colname, Object value) {
		Table table = new Table(schema);

		if (schema.getColumnIndex(colname) < 0) {
			throw new IllegalStateException("Error: table does not contain column "+colname);
		}

		for(Tuple t: tuples){
			if(t.get(colname).equals(value)){
				table.insert(t);
			}
		}
		return table;
	}
	
	@Override
	public Iterator<Tuple> iterator() {
		return new TIterator(tuples.iterator());
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (Tuple t: tuples) {
			sb.append(t.toString());  // one tuple per line
			sb.append("\n");          // new line character
		}
		return sb.toString();
	}

	/*
	 * An iterator that returns a copy of each tuple in 
	 * the table.
	 */
	public static class TIterator implements Iterator<Tuple> {
		
		private Iterator<Tuple> it;
		
		public TIterator(Iterator<Tuple> it) {
			this.it = it;
		}

		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public Tuple next() {
			return new Tuple(it.next());
		}	
	}
}
