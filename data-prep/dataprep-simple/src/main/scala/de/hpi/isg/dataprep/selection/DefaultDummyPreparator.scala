package de.hpi.isg.dataprep.selection

import org.apache.spark.sql.DataFrame

class DefaultDummyPreparator {
  def execute(df: DataFrame) = df
}
