package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata._
import de.hpi.isg.dataprep.model.target.objects.{FileMetadata, Metadata, TableMetadata}
import de.hpi.isg.dataprep.preparators.implementation.DefaultRemovePreambleImpl

/**
  *
  * @author Lasse Kohlmeyer
  * @since 2018/29/15
  */
class RemovePreamble(val delimiter: String, val hasHeader: String, val hasPreamble: Boolean, val rowsToRemove: Integer, val commentCharacter: String, val path: String) extends Preparator {

    def this(delimiter: String, hasHeader: String, rowsToRemove: Integer, path: String) = this(delimiter, hasHeader, true, rowsToRemove,"",path)

    def this(delimiter: String, hasHeader: String, commentCharacter: String,path: String) = this(delimiter, hasHeader, true, 0, commentCharacter,path)

    def this(delimiter: String, hasHeader: String,path: String) = this(delimiter, hasHeader, true, 0, "",path)

    def this(delimiter: String, hasHeader: String, hasPreamble:Boolean, rowsToRemove: Integer,path: String) = this(delimiter, hasHeader, hasPreamble, rowsToRemove,"",path)

    def this(delimiter: String, hasHeader: String, hasPreamble:Boolean, commentCharacter: String,path: String) = this(delimiter, hasHeader, hasPreamble, 0, commentCharacter,path)

    def this(delimiter: String, hasHeader: String, hasPreamble:Boolean,path: String) = this(delimiter, hasHeader, hasPreamble, 0, "",path)


    this.impl = new DefaultRemovePreambleImpl

    /**
      * This method validates the input parameters of a [[Preparator]]. If succeeds, setup the values of metadata into both
      * prerequisite and toChange set.
      *
      * @throws Exception
      */
    override def buildMetadataSetup(): Unit = {

        val prerequisites = new util.ArrayList[Metadata]
        val tochanges = new util.ArrayList[Metadata]

        if (delimiter == null) throw new ParameterNotSpecifiedException(String.format("Delimiter not specified."))
        if (hasHeader == null ) throw new ParameterNotSpecifiedException(String.format("No information about header"))

        prerequisites.add(new CommentCharacter(commentCharacter,new FileMetadata(path)))//optional
        prerequisites.add(new RowsToRemove(rowsToRemove,new FileMetadata(path)))//optional
        prerequisites.add(new Delimiter(delimiter,new TableMetadata("Default dataset name")))//neccesary
        prerequisites.add(new HeaderExistence(hasHeader.toBoolean, new TableMetadata("Default dataset name")))//neccesary
        prerequisites.add(new PreambleExistence(true, new FileMetadata(path)))//optional

        tochanges.add(new PreambleExistence(false,new FileMetadata(path)))

        this.prerequisites.addAll(prerequisites)
        this.updates.addAll(tochanges)
    }
}
