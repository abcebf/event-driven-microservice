package com.ebf.eventdriven;

/**
 * Created by hhuang on 8/4/16.
 */
public class EbfEvent<T> {
  private String producerLogNo;
  private boolean last;
  private T body;

  public String getProducerLogNo() {
    return producerLogNo;
  }

  public void setProducerLogNo(String producerLogNo) {
    this.producerLogNo = producerLogNo;
  }

  public boolean isLast() {
    return last;
  }

  public void setLast(boolean last) {
    this.last = last;
  }

  public T getBody() {
    return body;
  }

  public void setBody(T body) {
    this.body = body;
  }
}
