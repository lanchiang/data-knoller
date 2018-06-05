package de.hpi.isg.dataprep.parameter;

import de.hpi.isg.dataprep.model.entity.DataEntity;

import java.util.Map;

/**
 * @author Lan Jiang
 * @since 2018/6/5
 */
public class Parameter {

    /**
     * A entity represents the scope that a preparator influences.
     */
    private DataEntity entity;
    private Map<String, String> parameters;

    public DataEntity getEntity() {
        return entity;
    }

    public void setEntity(DataEntity entity) {
        this.entity = entity;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }
}
