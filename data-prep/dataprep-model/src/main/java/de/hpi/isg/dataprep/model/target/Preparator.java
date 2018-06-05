package de.hpi.isg.dataprep.model.target;

import de.hpi.isg.dataprep.parameter.Parameter;
import de.hpi.isg.dataprep.util.Executable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.*;

/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
abstract public class Preparator extends Target implements Executable {

    protected List<Errorlog> errorlogs;
    protected Set<Metadata> metadataRequisite;
    protected List<Errorlog> metadataRequisiteErrorlogs;
    protected Parameter parameters;

    protected Preparator() {
        this.metadataRequisite = new HashSet<>();
        this.errorlogs = new LinkedList<>();
    }

    abstract protected void configureMetadataRequisite();
    abstract protected void checkMetadataRequisite();
}
