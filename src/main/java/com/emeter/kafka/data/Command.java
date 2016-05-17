package com.emeter.kafka.data;

/**
 * Created by abhiso on 4/25/16.
 */
public class Command {
    private String switchTo;
    private Long meterId;

    public Command() {
    }

    public Command(String switchTo, Long meterId) {
        this.switchTo = switchTo;
        this.meterId = meterId;
    }

    public String getSwitchTo() {
        return switchTo;
    }

    public void setSwitchTo(String switchTo) {
        this.switchTo = switchTo;
    }

    public Long getMeterId() {
        return meterId;
    }

    public void setMeterId(Long meterId) {
        this.meterId = meterId;
    }
}
