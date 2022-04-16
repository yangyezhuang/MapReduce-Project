package com.base.partitionEmployee;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Employee implements Writable {
    private int empno;
    private String ename;
    private String job;
    private int mgr;
    private String hiredate;
    private int sal;
    private int comn;
    private int deptno;

    public String toString() {
        return "Employee [empno=" + empno + "，ename=" + ename + "，sal=" + sal + "，deptno=" + deptno + "]";
    }

    public void write(DataOutput output) throws IOException {
        output.writeInt(this.empno);
        output.writeUTF(this.ename);
        output.writeUTF(this.job);
        output.writeInt(this.mgr);
        output.writeUTF(this.hiredate);
        output.writeInt(this.sal);
        output.writeInt(this.comn);
        output.writeInt(this.deptno);
    }

    public void readFields(DataInput input) throws IOException {
        this.empno = input.readInt();
        this.ename = input.readUTF();
        this.job = input.readUTF();
        this.mgr = input.readInt();
        this.hiredate = input.readUTF();
        this.sal = input.readInt();
        this.comn = input.readInt();
        this.deptno = input.readInt();
    }

    public int getEmpno() {
        return empno;
    }

    public String getEname() {
        return ename;
    }

    public String getJob() {
        return job;
    }

    public int getMgr() {
        return mgr;
    }

    public String getHiredate() {
        return hiredate;
    }

    public int getSal() {
        return sal;
    }

    public int getComn() {
        return comn;
    }

    public int getDeptno() {
        return deptno;
    }

    public void setEmpno(int empno) {
        this.empno = empno;
    }

    public void setEname(String ename) {
        this.ename = ename;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public void setMgr(int mgr) {
        this.mgr = mgr;
    }

    public void setHiredate(String hiredate) {
        this.hiredate = hiredate;
    }

    public void setSal(int sal) {
        this.sal = sal;
    }

    public void setComn(int comn) {
        this.comn = comn;
    }

    public void setDeptno(int deptno) {
        this.deptno = deptno;
    }
}
