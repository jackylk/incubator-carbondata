package com.huawei.cloudtable.leo.hbase.codecs;

import com.huawei.cloudtable.leo.value.Date;
import com.huawei.cloudtable.leo.value.Time;
import org.junit.Assert;
import org.junit.Test;

public class DateCodecTest {

  @Test
  public void testDate() {
    Date date = Date.of("2019-6-7");
    DateCodec dateCodec = new DateCodec();
    byte[] encodedDate = dateCodec.encode(date);
    Date date2 = dateCodec.decode(encodedDate);

    Assert.assertEquals(date, date2);
  }
}
