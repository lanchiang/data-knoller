package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.{RemovePreamble}
import de.hpi.isg.dataprep.{ExecutionContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{_}
import org.apache.spark.sql._
import org.apache.spark.util.CollectionAccumulator

/**
  *
  * @author Lasse Kohlmeyer
  * @since 2018/11/29
  */
class DefaultRemovePreambleImpl extends PreparatorImpl {
    private var preambleCharacters="[!#$%&*+-./:<=>?@^|~].*$"
    override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {

        val preparator = abstractPreparator.asInstanceOf[RemovePreamble]
        val delimiter = preparator.delimiter
        val hasHeader= preparator.hasHeader.toBoolean
        val hasPreamble= preparator.hasPreamble
        var endPreambleIndex=preparator.rowsToRemove
        val commentCharacter=preparator.commentCharacter

        if(hasPreamble==false) return return new ExecutionContext(dataFrame, errorAccumulator)


        //identifying last preamble row
        if(endPreambleIndex==0){
            endPreambleIndex=findRow(dataFrame)
        }
        if(endPreambleIndex==0 && (hasPreambleInColumn(dataFrame)==false) ){
            return new ExecutionContext(dataFrame, errorAccumulator)
        }

        //taking char character as preamble character
        if (commentCharacter.equals("")==false){
            preambleCharacters=commentCharacter+".*$"
        }
        //drop all rows before last preamble row and update column names

        //creating new dataframe to frame of original dataframe
        val filteredDatasetReloaded=newDataFrameToPath(dataFrame.inputFiles(0),endPreambleIndex,dataFrame,delimiter, hasHeader)
        var filteredDataframe=filteredDatasetReloaded

        try{
            filteredDataframe=newHeadForDataFrame(removePreambleRecursive(dataFrame,endPreambleIndex),dataFrame)
        } catch {case e: Exception =>

            filteredDataframe
        }

        //if column size is identical use modified dataframe, else use new parsed dataframe
        if(filteredDataframe.columns.length!=filteredDatasetReloaded.columns.length){
            filteredDataframe=filteredDatasetReloaded
        }

        val ergDataset=filteredDataframe
        new ExecutionContext(ergDataset, errorAccumulator)
    }


    def removePreambleRecursive(dataFrame: DataFrame, line: Int): DataFrame ={
        if(line<=0){
            return dataFrame
        }
        else{
            var filteredDataset=removeFirstLine(dataFrame)

            removePreambleRecursive(filteredDataset,line-1)
        }
    }

    //removes  first row and its uplicates
    def removeFirstLine(dataFrame: DataFrame): DataFrame={
        val rdd = dataFrame.rdd.mapPartitionsWithIndex{
            case (index, iterator) => if(index==0) iterator.drop(1) else iterator
        }
        val spark=SparkSession.builder.master("local").getOrCreate()
        spark.createDataFrame(rdd, dataFrame.schema)
    }


    def removePreamble(dataFrame: DataFrame): DataFrame ={
        val row=findRow(dataFrame)
        val erg=removePreambleRecursive(dataFrame, row)
        erg
    }

    def numberOfIdenticalRowsRemoved(dataFrame: DataFrame): Integer ={

        val skipable_first_row = dataFrame.first()
        val filteredDataset=dataFrame.filter(row=>row==skipable_first_row)

        val erg=filteredDataset.count()
        erg.toInt
    }


    //removes all lines starting with commentg-charcter defined above ("-character is excluded)
    /*def removeAllCommentLikes(dataFrame: DataFrame): DataFrame ={
        val filteredDataset=dataFrame.filter(row=>row.get(0).toString().matches(preambleCharacters)==false)
        newHeadForDataFrame(filteredDataset,dataFrame)
        //filteredDataset
    }*/

    //creates a new datafram from the original dataframes path
    def newDataFrameToPath(path: String, skipRows:Integer, oldFrame:DataFrame, delimeter: String, hasHeader: Boolean): DataFrame ={
        val oldHeader=hasHeader
        val oldSeperator=delimeter

        val hasPreambleHeader=hasPreambleInColumn(oldFrame)
        var rowsToRemove=skipRows
        if (hasPreambleHeader)rowsToRemove+=1
        if (rowsToRemove==0) return oldFrame

        val spark=SparkSession.builder.master("local").getOrCreate()
        val sc = spark.sparkContext

        val textFile = sc.textFile(path).filter(row=>row.equals("")==false)

        val rowed: RDD[Row] = textFile.map(_.split(oldSeperator)).map {
            a: Array[_] => Row(a: _*)
        }

        val prefiltered=rowed.filter(row=>row.size>0)

        val filteredTextFile= removePreambleOfFile(prefiltered,rowsToRemove)

        try{
            var head=filteredTextFile.first()
            if(oldHeader==false) {
                head=Row.fromSeq(filteredTextFile.first().toSeq.zipWithIndex.map { case(element, index)=>"_c"+index}.toSeq)
            }
            val schema=this.schemaFromHeaderRow(head)
            val fileBody=filteredTextFile.filter(row=>row.equals(head)==false)
            val newDF= spark.createDataFrame(fileBody,schema)//.toDF("1","2","3","4","5","6","7","8","9")

            newDF
        }catch {
            case e: Exception => return oldFrame;
        }
    }

    //creates a schema, assuming the given row as columns
    def schemaFromHeaderRow(row: Row): StructType ={
        var schema = new StructType()

        var i=0
        for(i <- 0 to row.size-1){
            schema=schema.add(row(i).toString, StringType)
        }

        schema
    }

    //removes all rows up to specified index
    def removePreambleOfFile(file:RDD[Row], rows: Integer): RDD[Row]={

        val filteredTextFile=file.mapPartitionsWithIndex { (idx, iter) =>
            if (idx == 0) {
                iter.drop(rows)
            } else {
                iter
            }
        }
        filteredTextFile
    }

    //overwrite old header with first line of dataframe
    def newHeadForDataFrame(dataFrame: DataFrame,oldDataFrame:DataFrame): DataFrame={
        val header=oldDataFrame.columns
        val firstHeader=header(0)
        if(firstHeader.matches(preambleCharacters)){

            try{
                val newHeaderNames: Seq[String]=dataFrame.head().toSeq.asInstanceOf[Seq[String]]

                val newHeadedDataset=removeFirstLine(dataFrame.toDF(newHeaderNames: _*))
                newHeadedDataset
            }catch {
            case e: Exception =>
                return dataFrame.toDF("_c0");
        }

        }
        else{
            dataFrame
        }
    }

    //checks if the premable contains one of the preamble characters as first character

    def hasPreambleInColumn(dataFrame: DataFrame): Boolean={
        if(dataFrame.columns(0).matches(preambleCharacters)) true
        else false
    }

    //rekursive search for first row which is not preamble like
    def findRow(dataFrame: DataFrame): Integer={
        try{
            if(dataFrame.first().size<=0) return 0
        }catch {
            case e: Exception => return 0;
        }
        val firstString=dataFrame.first().get(0).toString()

        val rows = 1
        if(firstString.matches(preambleCharacters)){
            val reducedFrame=removeFirstLine(dataFrame)
            findRow(reducedFrame)+rows
        }
        else if(firstString.equals("")){
            val reducedFrame=removeFirstLine(dataFrame)
            findRow(reducedFrame)+rows
        }
        //check for "-character as first character
        else if(firstString.matches("['\"].*$")){
            val reducedFrame=removeFirstLine(dataFrame)
            findRow(reducedFrame)+rows
        }
        // else recursive abbort
        else
            0
    }


}
