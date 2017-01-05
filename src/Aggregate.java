package simpledb;

import java.util.*;

import simpledb.Aggregator.Op;

/**
 * An {@code Aggregate} operator computes an aggregate value (e.g., sum, avg, max, min) over a single column, grouped by a
 * single column.
 */
public class Aggregate extends AbstractDbIterator {

	Aggregator aggregator;
    DbIterator tupleIterator;
    DbIterator aggregateIterator;
    int indexOfAggregateField;
    int indexOfgroupByField;
    Aggregator.Op aggregateOperator;
    

	/**
	 * Constructs an {@code Aggregate}.
	 *
	 * Implementation hint: depending on the type of afield, you will want to construct an {@code IntAggregator} or
	 * {@code StringAggregator} to help you with your implementation of {@code readNext()}.
	 * 
	 *
	 * @param child
	 *            the {@code DbIterator} that provides {@code Tuple}s.
	 * @param afield
	 *            the column over which we are computing an aggregate.
	 * @param gfield
	 *            the column over which we are grouping the result, or -1 if there is no grouping
	 * @param aop
	 *            the {@code Aggregator} operator to use
	 */
	public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {

    this.tupleIterator = child;
    this.indexOfAggregateField = afield;
    this.indexOfgroupByField = gfield;
    this.aggregateOperator = aop;
	
	Type typeOfGfield = this.tupleIterator.getTupleDesc().getType(this.indexOfgroupByField);
	if(this.tupleIterator.getTupleDesc().getType(this.indexOfAggregateField).equals(Type.STRING_TYPE)){
		this.aggregator	= new StringAggregator(gfield, typeOfGfield, afield, aop);
	}else{
		this.aggregator = new IntAggregator(gfield, typeOfGfield, afield, aop);
	}
		}			

	public static String aggName(Aggregator.Op aop) {
		switch (aop) {
		case MIN:
			return "min";
		case MAX:
			return "max";
		case AVG:
			return "avg";
		case SUM:
			return "sum";
		case COUNT:
			return "count";
		}
		return "";
	}

	public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
		try {
			while (this.tupleIterator.hasNext()) {
				this.aggregator.merge(this.tupleIterator.next());
			}
		} catch (NoSuchElementException | DbException | TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		aggregateIterator = this.aggregator.iterator();		

		}
	

	/**
	 * Returns the next {@code Tuple}. If there is a group by field, then the first field is the field by which we are
	 * grouping, and the second field is the result of computing the aggregate, If there is no group by field, then the
	 * result tuple should contain one field representing the result of the aggregate. Should return {@code null} if
	 * there are no more {@code Tuple}s.
	 */
	protected Tuple readNext() throws TransactionAbortedException, DbException {
		if(aggregateIterator.hasNext()){
			aggregateIterator.next();
		}
		return null;
	}

	public void rewind() throws DbException, TransactionAbortedException {
		aggregateIterator.rewind();
	}

	/**
	 * Returns the {@code TupleDesc} of this {@code Aggregate}. If there is no group by field, this will have one field
	 * - the aggregate column. If there is a group by field, the first field will be the group by field, and the second
	 * will be the aggregate value column.
	 * 
	 * The name of an aggregate column should be informative. For example:
	 * {@code aggName(aop) (child_td.getFieldName(afield))} where {@code aop} and {@code afield} are given in the
	 * constructor, and {@code child_td} is the {@code TupleDesc} of the child iterator.
	 */
	public TupleDesc getTupleDesc() {
		return aggregateIterator.getTupleDesc();
	}

	public void close() {
		aggregateIterator.close();	
		}
}
