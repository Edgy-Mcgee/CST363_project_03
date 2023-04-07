package heapdb;

import java.util.ArrayList;

public class LSMindex {
	
	private class Entry {
		private Object key;
		private int blockNo;
		
		private Entry(Object k, int b) {
			key=k;
			blockNo=b;
		}
	}
	
	private ArrayList<Entry> entries = new ArrayList<>();
	
	public void append(Object key, int blockNo) {
		entries.add(new Entry(key, blockNo));
	}
	
	/*
	 * binary search of index array.
	 * return  -1 index is empty, or key is < lowest key value.
	 *        int blockNumber to search for tuple.
	 */
	public int lookup(Object key) {
		int left = 0;
		int right = entries.size() - 1;

		Object lowestKey = entries.get(left).key;
		int cmp = Tuple.compareKeys(lowestKey, key);
		if (cmp > 0) // key is lower than lowest entry key
			return -1;

		int closestButLess = 0;

		while (left <= right){
			int middle = left + (right - left) / 2;
			Object midKey = entries.get(middle).key;

			cmp = Tuple.compareKeys(midKey, key);

			if(cmp == 0) //midKey == key
				return entries.get(middle).blockNo;
			if(cmp < 0) { //midKey < key
				left = middle + 1;
				closestButLess = middle;
			}else{ //midKey > key
				right = middle - 1;
			}
		}
		return closestButLess;
	}
	
	public void printDiagnostic() {
		 System.out.println("Begin index entries.");
		 for (Entry e: entries) {
			 System.out.printf("%d %d \n",e.key, e.blockNo);
		 }
		 System.out.println("End index entries.");
	}

}
