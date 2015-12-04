package edu.neu.mr.amazon.reviews.utils;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class WritableParent extends GenericWritable {

	  private final static Class[] CLASSES =  new Class[]{
	    RatingPairWritable.class,
	    Text.class
	  };
	  
	  public WritableParent(){    
	  }
	  
	  public WritableParent(Writable value){
	    set(value);
	  }
	  
	  protected Class[] getTypes() {
	    return CLASSES;
	  }
}