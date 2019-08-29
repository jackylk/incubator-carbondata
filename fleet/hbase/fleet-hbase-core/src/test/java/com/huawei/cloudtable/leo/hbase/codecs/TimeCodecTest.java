package com.huawei.cloudtable.leo.hbase.codecs;

import com.huawei.cloudtable.leo.value.Time;
import org.junit.Assert;
import org.junit.Test;

public class TimeCodecTest {

  @Test
  public void testTime() {
    Time time = Time.of("19:06:07");
    TimeCodec timeCodec = new TimeCodec();
    byte[] encodedTime = timeCodec.encode(time);
    Time time2 = timeCodec.decode(encodedTime);

    Assert.assertEquals(time, time2);
  }
}
