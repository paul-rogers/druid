package org.apache.druid.query.profile;

public class ProfileUtils
{
  private ProfileUtils()
  {
    
  }
  
  public static String classOf(Object obj) {
    return obj == null ? null : obj.getClass().getSimpleName();
  }
}
