package org.apache.carbondata.router;

import org.apache.spark.sql.catalyst.TableIdentifier;

public interface CarbonQueryRunner {
  void runCarbon(Destination destination, TableIdentifier resultTable);
}
