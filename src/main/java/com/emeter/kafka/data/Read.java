package com.emeter.kafka.data;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Date;

/**
 * Created by abhiso on 4/25/16.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "type")
public class Read {
    private Long meterId;
    private Long measTypeId;
    private Double value;
    private Date readTime;
    private Long flag;

    public Read() {
    }

    public Read(Long meterId, Long measTypeId, Double value, Date readTime, Long flag) {
        this.meterId = meterId;
        this.measTypeId = measTypeId;
        this.value = value;
        this.readTime = readTime;
        this.flag = flag;
    }

    public Long getMeasTypeId() {
        return measTypeId;
    }

    public void setMeasTypeId(Long measTypeId) {
        this.measTypeId = measTypeId;
    }

    public Long getMeterId() {
        return meterId;
    }

    public void setMeterId(Long meterId) {
        this.meterId = meterId;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public Date getReadTime() {
        return readTime;
    }

    public void setReadTime(Date readTime) {
        this.readTime = readTime;
    }

    public Long getFlag() {
        return flag;
    }

    public void setFlag(Long flag) {
        this.flag = flag;
    }
}
