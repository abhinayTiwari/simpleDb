package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
/**
 * 
 * @author pritee
 *
 * @param <Tuple>
 */
public class MyIterator<Tuple> implements Iterator<Tuple> {
	Iterator<Tuple> t;
	ArrayList<Tuple> tuple;
	public MyIterator(ArrayList<Tuple> tuple){
		this.tuple=tuple;
		t = tuple.iterator();
		
	}
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return t.hasNext();
	}

	@Override
	public Tuple next() {
		// TODO Auto-generated method stub
		return t.next();
	}

}
