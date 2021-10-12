package org.apache.druid.query.profile;

public class Timer
{
  private long totalTime;
  private long startTime;
  
  public Timer() {  
  }
  
  public static Timer create() {
    return new Timer();
  }
  
  public static Timer createStarted() {
    Timer timer = create();
    timer.start();
    return timer;
  }
  
  public void start() {
    if (startTime == 0) {
      startTime = System.nanoTime();
    }
  }
  
  public void stop() {
    if (startTime != 0) {
      totalTime += System.nanoTime() - startTime;
      startTime = 0;
    }
  }
  
  public long get() {
    stop();
    return totalTime;
  }
}
