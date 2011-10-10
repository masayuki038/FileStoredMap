package net.wrap_trap.utils;

import java.util.Date;

import org.msgpack.annotation.MessagePackBeans;
import org.msgpack.annotation.MessagePackMessage;

@MessagePackBeans
public class Employee {

	private int a;
	private String name;
	private int sal;
	//private Date date;

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
//	public Date getDate() {
//		return date;
//	}
//	public void setDate(Date date) {
//		this.date = date;
//	}
	public int getA() {
		return a;
	}
	public void setA(int a) {
		this.a = a;
	}
}
