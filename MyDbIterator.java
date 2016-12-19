package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
/**
 * 
 * @author pritee
 *
 * @param <Tuple>
 */
public class MyDbIterator implements DbFileIterator {
	Iterator<Tuple> t;
	List<Tuple> tuple;
	public MyDbIterator(List<Tuple> l){
		this.tuple=l;
		t = l.iterator();
		
	}
	@Override
	public void open() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		// TODO Auto-generated method stub
		return t.next();
	}
	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return t.hasNext();
	}


}
