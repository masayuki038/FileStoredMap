package net.wrap_trap.collections.fsm;

import java.io.Serializable;
import java.util.Date;

public class Employee implements Serializable {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -4376357314587798576L;

    private String name;
    private int sal;
    private Date createdAt;

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

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }
}
