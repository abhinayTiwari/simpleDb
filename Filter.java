package simpledb;  

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends AbstractDbIterator {

	/**
	 * The child operator.
	 */
	DbIterator child;
	
	/**
	 * The predicate.
	 */
	Predicate p;
	
    /**
     * Constructor accepts a predicate to apply and a child
     * operator to read tuples to filter from.
     *
     * @param p The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
    	this.child = child;	
    	this.p = p;
    	
    }

    public TupleDesc getTupleDesc() {
    	return child.getTupleDesc();
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
    	child.open();
    }

    public void close() {
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	child.rewind();   	
    }

    /**
     * AbstractDbIterator.readNext implementation.
     * Iterates over tuples from the child operator, applying the predicate
     * to them and returning those that pass the predicate (i.e. for which
     * the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no more tuples
     * @see Predicate#filter
     */
    protected Tuple readNext(){
    	try {
			while(child.hasNext()){
				Tuple t = child.next();
				boolean check = this.p.filter(t);
				if(check){
					return t; 
				}
			}
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
        return null;
    }
}
