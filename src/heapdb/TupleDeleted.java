package heapdb;

/*
 * Marker class for a deleted tuple.
 * Contains only the key value of the deleted tuple.
 */
class TupleDeleted extends Tuple {

	TupleDeleted(Schema schema, Object key) {
		super(schema);
		int colIndex = schema.getColumnIndex(schema.getKey());
		this.set(colIndex, key);
	}
	
}
