package net.wrap_trap.utils;

import java.io.Serializable;

public class Employee implements Serializable{
	
	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = -4376357314587798576L;

	private int a;
	private String name;
	private int sal;
	
	public int getA() {
		return a;
	}
	public void setA(int a) {
		this.a = a;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getSal() {
		return sal;
	}
	public void setSal(int sal) {
		this.sal = sal;
	}
}
