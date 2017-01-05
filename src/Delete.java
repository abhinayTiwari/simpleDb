package simpledb;  

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {
	 TransactionId transactionId;
	 DbIterator tupleIterator;
	
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.transactionId = t;
    	this.tupleIterator = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return null;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	tupleIterator.open();
    }

    public void close() {
        // some code goes here
    	tupleIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	tupleIterator.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	 int counter = 0;
    	 while (tupleIterator.hasNext()){
             Tuple tup=tupleIterator.next();
             Database.getBufferPool().deleteTuple(this.transactionId, tup);
             counter=counter+1;
         }
    	//creating a 1-field tuple  containing the number of deleted records
    	 
    	Field ifield = new IntField(counter);
    	Tuple resultingTuple = new Tuple(new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { "number of deleted records" }));
    	if(counter>0){
        resultingTuple.setField(0, ifield);
    	}
        return resultingTuple;
    }
}
