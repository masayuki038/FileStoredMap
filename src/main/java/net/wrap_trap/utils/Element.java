package net.wrap_trap.utils;

public class Element<V> {
	private String className;
	private V value;
	
	public Element(){
	}
	
	public Element(V v) {
		super();
		this.className = v.getClass().getName();
		this.value = v;
	}
	
	public V getValue() {
		return value;
	}
	
	public void setValue(V v){
		this.value = v;
	}
	
	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}	
}
