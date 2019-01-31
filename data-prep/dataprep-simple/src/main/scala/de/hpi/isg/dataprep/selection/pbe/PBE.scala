package de.hpi.isg.dataprep.selection.pbe

import org.apache.spark.sql.{Dataset, Row}

case class PBE(original: Dataset[Row], target: Dataset[Row])