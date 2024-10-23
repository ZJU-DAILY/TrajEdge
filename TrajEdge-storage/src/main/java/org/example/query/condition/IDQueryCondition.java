package org.example.query.condition;

/**
 * @author Haocheng Wang
 * Created on 2023/2/14
 */
public class IDQueryCondition extends AbstractQueryCondition{

  String moid;

  public IDQueryCondition(String moid) {
    this.moid = moid;
  }

  public String getMoid() {
    return moid;
  }

  @Override
  public String getConditionInfo() {
    return "IDQueryCondition{" +
        "moid='" + moid + '\'' +
        '}';
  }
}
