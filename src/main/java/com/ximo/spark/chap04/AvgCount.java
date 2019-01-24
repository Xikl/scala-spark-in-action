package com.ximo.spark.chap04;

import java.io.Serializable;

/**
 * @author 朱文赵
 * @date 2019/1/24 15:03
 */
public class AvgCount implements Serializable {

    private int total;

    private int num;

    public AvgCount(int total, int num) {
        this.total = total;
        this.num = num;
    }

    public float avg() {
        return total / (float)num;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}
