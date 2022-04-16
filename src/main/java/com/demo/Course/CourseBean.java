package com.demo.Course;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CourseBean implements WritableComparable<CourseBean> {

    private String course;
    private String student;
    private Float score;

    public CourseBean() {

    }

    public CourseBean(String course, String student, Float score) {
        this.course = course;
        this.student = student;
        this.score = score;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(course);
        out.writeUTF(student);
        out.writeFloat(score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        course = in.readUTF();
        student = in.readUTF();
        score = in.readFloat();
    }

    @Override
    public int compareTo(CourseBean o) {
        return o.score > score ? -1 : 1;
    }

    @Override
    public String toString() {
        return course + ", " + student + ", " + score;
    }

    public String getCourse() {
        return course;
    }

//    public String getStudent() {
//        return student;
//    }
//
//    public Float getScore() {
//        return score;
//    }
}
