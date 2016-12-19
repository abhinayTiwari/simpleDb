package simpledb;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import simpledb.Aggregator.Op;

/**
 * An {@code IntAggregator} computes some aggregate value over a set of {@code IntField}s.
 */
public class IntAggregator implements Aggregator {
	  
		int indexOfGbField;
		Type typeOfGbField;
		int indexOfAfield;
		Op aggregatOperator;
		TupleDesc tupleDescripton;
		List<Tuple> totalTupleList = new LinkedList<Tuple>();
		int counter;
 
	
	
	/**
	 * Constructs an {@code Aggregate}.
	 * 
	 * @param gbfield
	 *            the 0-based index of the group-by field in the tuple, or {@code NO_GROUPING} if there is no grouping
	 * @param gbfieldtype
	 *            the type of the group by field (e.g., {@code Type.INT_TYPE}), or {@code null} if there is no grouping
	 * @param afield
	 *            the 0-based index of the aggregate field in the tuple
	 * @param what
	 *            the aggregation operator
	 */

	public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		
		this.indexOfGbField = gbfield;
		this.typeOfGbField = gbfieldtype;
		this.indexOfAfield = afield;
		this.aggregatOperator = what;
		
		
		if (this.typeOfGbField == null) {
			Type[] typeOfTuple = { Type.INT_TYPE };
			this.tupleDescripton = new TupleDesc(typeOfTuple);
		} else {
			Type[] typeOfTuple = { gbfieldtype, Type.INT_TYPE };
			this.tupleDescripton = new TupleDesc(typeOfTuple);
		}
	}

	/**
	 * Merges a new {@code Tuple} into the aggregate, grouping as indicated in the constructor.
	 * 
	 * @param tup
	 *            the {@code Tuple} containing an aggregate field and a group-by field
	 */
	public void merge(Tuple tup) {
		
		switch (this.aggregatOperator) {
	
		case COUNT:
			counter(tup);
			break;
		default:
			;

		}
	}

	
	public void counter(Tuple tup){
        Tuple newTuple = new Tuple(this.tupleDescripton);
		this.counter += 1;
		if (this.typeOfGbField == null) {
			newTuple.setField(0, new IntField(this.counter));
		} else {
			newTuple.setField(0, newTuple.getField(this.indexOfGbField));
			newTuple.setField(1, new IntField(this.counter));
		}
		totalTupleList.add(newTuple); 	
	}
	/**
	 * Creates a {@code DbIterator} over group aggregate results.
	 *
	 * @return a {@code DbIterator} whose tuples are the pair ({@code groupVal}, {@code aggregateVal}) if using group,
	 *         or a single ({@code aggregateVal}) if no grouping. The {@code aggregateVal} is determined by the type of
	 *         aggregate specified in the constructor.
	 */
	public DbIterator iterator() {
		// some code goes here
		IAIterator intAggregatorIterator =new IAIterator(this.totalTupleList);
		return intAggregatorIterator;
	}

	
	public static class IAIterator implements DbIterator {
		List<Tuple> listValue;
		Iterator<Tuple> iterator;

		public IAIterator(List<Tuple> listValue) {
			this.listValue = listValue;			
		}
	    public void open(){
	    	this.iterator = this.listValue.iterator();
	    }
		public boolean hasNext(){
			return this.iterator.hasNext();
		}

		public Tuple next(){
			return this.iterator.next();
		}
		public void rewind(){
			close();
			open();
		}
		public void close() {
			this.iterator = null;
		}
		@Override
		public TupleDesc getTupleDesc() {
			// TODO Auto-generated method stub
			return null;
		}

	}

}
