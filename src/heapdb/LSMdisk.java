package heapdb;

import static heapdb.Constants.BLOCK_SIZE;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class LSMdisk implements Iterable<Tuple> {
	
	public static boolean USE_INDEX = true;
	
	private Schema schema;
	private BlockedFile bfile;
	private String filename;
	private LSMindex index;

	/**
	 * Create new file and write schema to block 0.
	 */
	public LSMdisk(String filename, Schema schema) {
		this.schema=schema;
		this.filename = filename;
		ByteBuffer block0  = ByteBuffer.wrap(new  byte[Constants.BLOCK_SIZE]);
		block0.position(8);
		bfile = new BlockedFile(filename+".lsm", true);
		schema.serialize(block0);
		bfile.writeBlock(0,block0);
		index = new LSMindex();
	}
	
	/**
	 * Open existing file and read schema from block 0.
	 */
	public LSMdisk(String filename) {
		this.filename = filename;
		bfile = new BlockedFile(filename+".lsm", false);
		ByteBuffer block0  = ByteBuffer.wrap(new  byte[Constants.BLOCK_SIZE]);
		bfile.readBlock(0,block0);
		this.schema = Schema.deserialize(block0);
		buildIndex();		
	}
	
	public Schema getSchema() {
		return schema;
	}
	
	public int getHighestBlockNo() {
		return bfile.getHighestBlockNo();
	}
	
	public void close() {
		bfile.close();

	}
	
	/**
	 * lookup key in level 1 file.
	 */
	public Tuple lookup(Object key)  {

		return (USE_INDEX) ? indexLookup(key) : scanFileLookup(key);
	}
	
	private Tuple scanFileLookup(Object key) {
		/* 
		 * use LSMiterator to read all tuples in file and search for key. 
		 */
		for (Tuple t: this) {
			if (t.getKey().equals(key)) {
				return t;
			}
		}
		return  null;
	}
	
	private Tuple indexLookup(Object key) {
		int blockno = index.lookup(key);
		if (blockno < 0) return null;  // key does not exist
		//TODO 
		//  read and search file block   
		return null;
	} 
	
	
	/**
	 * merge data in level0 memory TreeMap 
	 * with existing level 1 data file. 
	 * Write out merged to data to a temporary file.
	 * Then delete data file and rename temporary file to data file.
	 */
	boolean merge(LSMmemory level0) {
		// create LSMdisk file to write to
		LSMdisk tempFile = new LSMdisk(filename+".temp", schema);
		LSMWriter bw = tempFile.getWriter();

		// get iterators over TreeMap and current LSMdisk file
		Iterator<Tuple> it0 = level0.iterator();
		Iterator<Tuple> it1 = this.iterator();

		// get the next tuple from each iterator or null if no more tuples.
		Tuple t0 = (it0.hasNext()) ? it0.next(): null;
		Tuple t1 = (it1.hasNext()) ? it1.next(): null;

		// while there is a next tuple
		while (t0!=null || t1!=null) {
			if (t0==null) {
				// TreeMap is empty.
				// copy remaining tuples from disk file.
				bw.append(t1);
				while (it1.hasNext()) {
					bw.append(it1.next());
				}
				break;
			} else if (t1==null) {
				// disk file is empty.
				// copy remaining tuples (except for TupleDeleted) from TreeMap
				if (!(t0 instanceof TupleDeleted)) bw.append(t0);
				while (it0.hasNext()) {
					t0=it0.next();
					if (!(t0 instanceof TupleDeleted)) bw.append(t0);
				}
				break;
			} else {
				// compare tuple keys
				int rc = Tuple.compareKeys(t0.getKey(), t1.getKey());
				if (rc < 0) {
					// write out t0 (unless TupleDeleted)
					if (!(t0 instanceof TupleDeleted)) bw.append(t0);
					t0 = (it0.hasNext()) ? it0.next(): null;
				} else if (rc==0) {
					// keys are equal.
					// write out t0 (unless TupleDeleted)
					if (!(t0 instanceof TupleDeleted)) bw.append(t0);
					t0 = (it0.hasNext()) ? it0.next(): null;
					t1 = (it1.hasNext()) ? it1.next(): null;
				} else {
					// write out t1.  disk file never contains TupleDeleted
					bw.append(t1);
					t1 = (it1.hasNext()) ? it1.next(): null;
				}
			}
		}

		bw.flush();  // close temp file.
		tempFile.close();
		bfile.close();  // close disk file

		// delete disk file and rename temp file to disk file.
		File fd = new File(filename+".lsm");
		boolean rc = fd.delete();
		if (!rc) throw new RuntimeException("Merge failed. Unable to delete "+filename+".lsm");

		File fm = new File(filename+".temp.lsm");
		rc = fm.renameTo(fd);
		if (!rc) throw new RuntimeException("Merge failed. Unable to rename temp file");

		bfile = new BlockedFile(filename+".lsm", false);  // open the new disk file.
		buildIndex();
		return true;
	}


	/*
      * build sparse index over first tuple in each disk block
      */
     void buildIndex() {
    	 index = new LSMindex();
    	 ByteBuffer byte_buffer = ByteBuffer.wrap(new byte[BLOCK_SIZE]);
    	 int start = 1;
    	 int end = bfile.getHighestBlockNo();
    	 for (int block=start; block<=end; block++) {
    		 bfile.readBlock(block, byte_buffer);
 			 Tuple t = Tuple.deserialize(schema, byte_buffer);
 			 index.append(t.getKey(), block);
    	 }
    	 if (Constants.DEBUG) {
	    	 System.out.println("After build index");
	    	 printDiagnostic();
    	 }
    }

	@Override 
	public LSMIterator iterator() {
		return new LSMIterator();
	}
	
	public LSMWriter getWriter() {
		return new LSMWriter();
	}
	
	public class LSMWriter {
		private ByteBuffer byte_buffer;
		
		public LSMWriter() {
			byte_buffer = ByteBuffer.wrap(new byte[BLOCK_SIZE]);
			byte_buffer.position(8);
		}
		
		/**
		 * flush current buffer unless current buffer is empty
		 */
		public void flush() {
			if (byte_buffer.position()> 8) bfile.appendBlock(byte_buffer);
		}
		
		/**
		 * Add a tuple to end of file.
		 * return true 
		 */
		public boolean append(Tuple t) {
			if (t.length() > this.byte_buffer.remaining()) {
				// not enough room for tuple in current buffer
				// write out current buffer and start new buffer.
				bfile.appendBlock(byte_buffer);
				
				// clear the buffer
				byte[] bytes = byte_buffer.array();
				for (int i=0; i<BLOCK_SIZE; i++) 
					bytes[i]=0;
				byte_buffer.position(8);
				
			}
			t.serialize(byte_buffer);
			return true;
		}

	}
	
	public class LSMIterator implements Iterator<Tuple> {
		
		private ByteBuffer byte_buffer;
		private int blockNo = 1;  // block number in buffer.
		private Tuple t;
		
		public LSMIterator() {
			byte_buffer = ByteBuffer.wrap(new byte[BLOCK_SIZE]);
			if (blockNo <= bfile.getHighestBlockNo()) {
				bfile.readBlock(blockNo, byte_buffer);
				t = getNextTuple();
			} else {
				// at end of file. no more data.
				byte_buffer.position(BLOCK_SIZE);
			}
		}
		
		private Tuple getNextTuple() {
			Tuple t = new Tuple(schema);
			for (int icol=0; icol < schema.size(); icol++) {
				switch (schema.getType(icol)) {
					case Constants.INT_TYPE:
						t.set(icol, byte_buffer.getInt());
						break;
					case Constants.VARCHAR_TYPE:
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
		
		int blockNo() {
			return blockNo;   // block that contained tuple
		}
	

		@Override
		public boolean hasNext() {
			
			if (t!=null) 
				return true;
			
			if (! byte_buffer.hasRemaining()) {
				// no more data in this buffer.  Read next block.
				blockNo++;
				if (blockNo <= bfile.getHighestBlockNo()) {
					bfile.readBlock(blockNo, byte_buffer);
				} else {
					// at end of file. no more data.
					return false;
					
				}
				
			}
			// get next tuple from current buffer
			t= getNextTuple();
			return true;
		}

		@Override
		public Tuple next() {
			if (t!=null) {
				Tuple r=t;
				t=null;
				return r;
			} else {
				if (hasNext()) return next();
				else return null;
			}
		}

	}
	
	public void printDiagnostic() {
		
		int lastBlockNo = bfile.getHighestBlockNo();
		System.out.printf("LSM disk block high %d\n", lastBlockNo);
		// print the LSM disk data file contents
		ByteBuffer byte_buffer = ByteBuffer.wrap(new byte[BLOCK_SIZE]);
		for (int blockno = 1; blockno<=lastBlockNo; blockno++) {
			bfile.readBlock(blockno, byte_buffer);
			System.out.printf("LSM block %d bytes used %d\n", blockno, byte_buffer.getInt(0));
			int recno=0;
			while (byte_buffer.hasRemaining()) {
				Tuple t = Tuple.deserialize(schema, byte_buffer);
				System.out.printf(" Rec %d %s\n", recno, t);
				recno++;
			}
		}
		
		if (index!=null) index.printDiagnostic();
	}
}