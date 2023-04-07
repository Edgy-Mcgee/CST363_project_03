package heapdb;

import java.nio.ByteBuffer;
import static heapdb.Constants.INT_TYPE;
import static heapdb.Constants.VARCHAR_TYPE;

public class Tuple {
	
	protected Object[] values;
	protected Schema schema;    
	
	/**
	 * Create a tuple given a schema and column values.
	 * @param schema
	 * @param values
	 */
	public Tuple(Schema schema, Object ...values) {
		if (values.length != schema.size()) {
			throw new IllegalArgumentException("Error: Number of values does not match size of schema.");
		}
		this.schema = schema;
		this.values = new Object[schema.size()];
		for (int k=0; k<schema.size(); k++) {
			this.values[k] = values[k];
		}
		
	}
	
	/**
	 * Create a tuple with null values for all columns.
	 * @param schema
	 */
	public Tuple(Schema schema) {
		this.schema = schema;
		this.values = new Object[schema.size()];
	}
	
	/**
	 * Copy constructor
	 */
	public Tuple(Tuple rec) {
		this.schema=rec.schema;
		this.values = new Object[schema.size()];
		for (int k=0; k<schema.size(); k++) {
			this.values[k] = rec.values[k];
		}
	}
	
	/**
	 * Based on the schema for the joined table
	 * return a join of the given tuples
	 */
	public static Tuple joinTuple(Schema s, Tuple t1, Tuple t2) {
		Tuple r = new Tuple(s);
		int t1size = t1.getSchema().size();
		for (int k=0; k<s.size(); k++) {
			if (k<t1size) {
				r.values[k]=t1.values[k];
			} else {
				r.values[k]=t2.get(s.getName(k));
			}
		}
		return r;
	}
	
	/**
	 * get the schema of this tuple
	 */
	public Schema getSchema() {
		return schema;
	}
	
	/**
	 * number of bytes in serialized tuple
	 * each integer type is 4 bytes
	 * each string type is 4 + string length
	 */
	public int length() {
		int len=0;
		for (int i=0; i<schema.size(); i++) {
			if (schema.getType(i)==INT_TYPE) {
				len = len + 4;
			} else if (schema.getType(i)==VARCHAR_TYPE) {
				len = len + 4 + ((String)values[i]).length();
			}
		}
		return len;
	}
	
	/**
	 * Return the value of the ith column  i=0, 1, . . .   
	 */
	public Object get(int i) {	
		if (i < 0 || i > schema.size()) {
			throw new IllegalArgumentException("Error: invalid column index. "+i);
		}
		return values[i];
	}
	
	/**
	 * Return the integer value of the ith column    
	 */
	public int getInt(int i) {	
		if (i < 0 || i > schema.size()) {
			throw new IllegalArgumentException("Error: invalid column index. "+i);
		}
		if (schema.getType(i)!= INT_TYPE) {
			throw new IllegalArgumentException("Error: invalid column type. "+i);
		}
		int value = (Integer)values[i];
		return value;
	}
	
	/**
	 * Return the String value of the ith column    
	 */
	public String getString(int i) {	
		if (i < 0 || i > schema.size()) {
			throw new IllegalArgumentException("Error: invalid column index. "+i);
		}
		if (schema.getType(i)!= VARCHAR_TYPE) {
			throw new IllegalArgumentException("Error: invalid column type. "+i);
		}
		String value = (String)values[i];
		return value;
	}
	
	/**
	 * Return the value of column by column name 
	 */
	public Object get(String name) {
		int i = schema.getColumnIndex(name);
		if (i < 0) {
			throw new IllegalArgumentException("Error: invalid column name. "+name);
		}
		return get(i);
	}
	
	/**
	 * Set the value of the ith column 
	 */
	public void set(int i, Object value) {
		if (i < 0 || i >= schema.size()) {
			throw new IllegalArgumentException("Error: column index out of bounds.");
		}
		values[i] = value;
	}
	
	/**
	 * Return the value of the key column.
	 * key is single column value
	 */
	public Object getKey() {
		return get(schema.getKey());
	}
	
	/** 
	 * Return subset of column values based on column names in schema.
	 */
	public Tuple project(Schema schema) {
		Object[] values = new Object[schema.size()];
		for (int i=0; i<schema.size(); i++) {
			values[i] = this.values[this.schema.getColumnIndex(schema.getName(i))];
		}
		Tuple result = new Tuple(schema);
		result.values = values;
		return result;
	}
	
	/**
	 * read the buffer and return a Tuple.
	 */
	public static Tuple deserialize(Schema schema, ByteBuffer byte_buffer) {
		Tuple t = new Tuple(schema);
		for (int icol=0; icol < schema.size(); icol++) {
			switch (schema.getType(icol)) {
				case heapdb.Constants.INT_TYPE:
					t.set(icol, byte_buffer.getInt());
					break;
				case heapdb.Constants.VARCHAR_TYPE:
					int strlen = byte_buffer.getInt();
					byte[] bytes = new byte[strlen];
					byte_buffer.get(bytes);
					t.set(icol,  new String(bytes));
					break;
				default:
					throw new RuntimeException("Unknown column type. "+schema.getType(icol));
			}
		}
		return t;
	}
	
	/**
	 * write this tuple to the buffer.
	 */
	public void serialize(ByteBuffer byte_buffer) {
		for (int icol=0; icol<schema.size(); icol++) {
			switch (schema.getType(icol)) {
			case heapdb.Constants.INT_TYPE:
				byte_buffer.putInt((int)values[icol]);
				break;
			case heapdb.Constants.VARCHAR_TYPE:
				String s = (String)values[icol];
				byte_buffer.putInt(s.length());
				byte_buffer.put(s.getBytes());
				break;
			default:
				throw new RuntimeException("Unknown column type. "+schema.getType(icol));
			}
		}
	}
	
	/**
	 * Compare tuple keys which can be Integer or String values.
	 * return negative number if a < b
	 *        0  if a == b
	 *        positive number if a > b
	 */
	public static int compareKeys(Object a, Object b) {
		if (a instanceof Integer && b instanceof Integer) {
			Integer i1 = (Integer)a;
			Integer i2 = (Integer)b;
			return i1.compareTo(i2);
		} else if (a instanceof String && b instanceof String) {
			String s1=(String)a;
			String s2=(String)b;
			return s1.compareTo(s2);
		} else {
			throw new IllegalArgumentException("Invalid key comparision "+a+" , "+b);
		}
	}

	/**
	 * Return a printable string from this tuple
	 */
	@Override
	public String toString() {
		StringBuilder s = new StringBuilder("[");
		for (int i=0; i<values.length; i++) {
			s.append(values[i]);
			if (i < values.length-1) s.append(", ");
		}
		s.append("]");
		return s.toString();
	}
}
