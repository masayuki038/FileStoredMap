package net.wrap_trap.utils;

public class Element<V> {
	
	public static final String VALUE = "value";
	public static final String TYPE = "type";
	
	private String type;
	private String key;
	private V value;
	
	public Element(String key, V v) {
		super();
		this.key = key;
		this.value = v;
		this.type = v.getClass().getName();
	}
	
	public V getValue() {
		return value;
	}
	
	public String getKey(){
		return key;
	}
	
	public String getType() {
		return type;
	}
}
