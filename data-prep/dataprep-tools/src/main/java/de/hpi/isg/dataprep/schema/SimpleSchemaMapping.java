package de.hpi.isg.dataprep.schema;

import de.hpi.isg.dataprep.model.target.schema.Attribute;
import de.hpi.isg.dataprep.model.target.schema.Schema;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
import de.hpi.isg.dataprep.model.target.schema.Transform;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class represents the schema mapping from the current schema to the target schema. It is used by the decision engine
 * to calculate the applicability score for preparator suggestion.
 *
 * @author lan.jiang
 * @since 12/18/18
 */
public class SimpleSchemaMapping extends SchemaMapping {

    private final Schema sourceSchema;
    private Schema currentSchema;
    private Schema targetSchema;

    /**
     * Maps from the currentSchema to the targetSchema.
     */
    private SchemaMappingNode root;

    public SimpleSchemaMapping(Schema sourceSchema, Schema targetSchema,
                               SchemaMappingNode root) {
        this(sourceSchema);
        this.targetSchema = targetSchema;
        this.root = root;
    }

    public SimpleSchemaMapping(Schema sourceSchema) {
        this.sourceSchema = sourceSchema;
        this.currentSchema = sourceSchema;

        this.root = new SchemaMappingNode(null, 0);
        List<SchemaMappingNode> sourceSchemaLayer = new LinkedList<>();
        for (Attribute attribute : this.currentSchema.getAttributes()) {
            SchemaMappingNode node = new SchemaMappingNode(attribute);
            node.update();
            sourceSchemaLayer.add(node);
        }
        this.root.next = sourceSchemaLayer;
    }

    @Override
    public Schema getCurrentSchema() {
        return currentSchema;
    }

    @Override
    public void setCurrentSchema(Schema schema) {
        this.currentSchema = schema;
    }

    @Override
    public Schema getTargetSchema() {
        return targetSchema;
    }

    @Override
    public boolean hasMapped() {
        return targetSchema.equals(currentSchema);
    }

    @Override
    public Set<Attribute> getTargetBySourceAttributeName(String attributeName) {
        if (attributeName == null) {
            throw new IllegalArgumentException("Attribute name is not set.");
        }
        Attribute attribute = currentSchema.getAttributes().stream()
                .filter(attributeInSchema -> attributeInSchema.getName().equals(attributeName)).findFirst().orElse(null);
        if (attribute == null) {
            throw new RuntimeException("The specified attribute does not exist.");
        }

        // this cannot find the attributes created in the previous steps.
        SchemaMappingNode attributeNode = root.next.stream()
                .filter(node -> node.getAttribute().getName().equals(attributeName)).findFirst().orElse(null);

        if (attributeNode == null) {
            throw new RuntimeException("The attribute node does not exist.");
        }
        List<SchemaMappingNode> resultNodes = excludeNodeInPreviousLayer(findLastNodesOfChain(attributeNode));
        if (resultNodes == null) {
            return new HashSet<>();
        }
        // if the current schema contains the attributes to be returned, do not return them (because they have been processed).
        resultNodes = resultNodes.stream().filter(node -> currentSchema.getAttributes().contains(node.attribute)).collect(Collectors.toList());

        return resultNodes.stream().map(node -> node.getAttribute()).collect(Collectors.toSet());
    }

    @Override
    public void finalizeUpdate() {
        updateSchemaMappingNodes();
        updateSchema();
    }

    private void updateSchemaMappingNodes() {
        root.next.stream()
                .map(node -> findLastNodesOfChain(node))
                .flatMap(lastNodes -> lastNodes.stream())
                .forEach(node -> {
                    if (!node.updated) {
                        node.update();
                    }
                });
    }

    @Override
    public void updateMapping(Attribute sourceAttribute, Attribute targetAttribute) {
        if (sourceAttribute == null) {
//            throw new RuntimeException("Source attribute can not be found in the current schema.");

            // source attribute is null means this is a add attribute transform.
            if (targetAttribute == null) {
                throw new RuntimeException("Unexpected argument setting.");
            } else {
                List<SchemaMappingNode> tails = root.next.stream().map(node -> findLastUpdatedNodesOfChain(node))
                        .flatMap(lastNodes -> lastNodes.stream())
                        .collect(Collectors.toList());
                int currentMaxLayer = currentMaxLayer(tails);
                root.next.add(new SchemaMappingNode(targetAttribute, currentMaxLayer+1));
            }
        }
        else {
            if (targetAttribute != null) {
                List<SchemaMappingNode> tails = root.next.stream().map(node -> findLastUpdatedNodesOfChain(node))
                        .flatMap(lastNodes -> lastNodes.stream())
                        .collect(Collectors.toList());
                tails = excludeNodeInPreviousLayer(tails);
                SchemaMappingNode sourceNode = tails.stream()
                        .filter(node -> node.attribute.equals(sourceAttribute))
                        .findFirst()
                        .get();
                if (sourceNode.next == null) {
                    sourceNode.next = new LinkedList<>();
                }
                sourceNode.next.add(new SchemaMappingNode(targetAttribute, sourceNode.getLayer()+1));
            } else {
                // target is null, saying that a delete transform was just executed.
                // pass
            }
        }
    }

    @Override
    protected void updateSchema() {
        List<SchemaMappingNode> tails = root.next.stream()
                .map(node -> findLastUpdatedNodesOfChain(node))
                .flatMap(lastNodes -> lastNodes.stream())
                .distinct()
                .collect(Collectors.toList());
        tails = excludeNodeInPreviousLayer(tails);

        List<Attribute> latestAttribute = new LinkedList<>();
        tails.stream().forEachOrdered(node -> {
            latestAttribute.add(node.getAttribute());
        });
        this.currentSchema = new Schema(latestAttribute);
    }

    @Override
    public SchemaMapping createSchemaMapping() {
        this.targetSchema = currentSchema;
        SchemaMapping newInstance = new SimpleSchemaMapping(this.sourceSchema, this.targetSchema, root);
        return newInstance;
    }

    private List<SchemaMappingNode> excludeNodeInPreviousLayer(List<SchemaMappingNode> schemaMappingNodes) {
        if (schemaMappingNodes.size() == 0) {
            return null;
        }
        int maxLayer = currentMaxLayer(schemaMappingNodes);
        schemaMappingNodes = schemaMappingNodes.stream().filter(node -> node.getLayer() == maxLayer).collect(Collectors.toList());
        return schemaMappingNodes;
    }

    private int currentMaxLayer(List<SchemaMappingNode> schemaMappingNodes) {
        if (schemaMappingNodes.size()==0) {
            return 0;
        }
        return schemaMappingNodes.stream().max(Comparator.comparingInt(node -> node.getLayer())).get().getLayer();
    }

    private static List<SchemaMappingNode> findLastUpdatedNodesOfChain(SchemaMappingNode node) {
        List<SchemaMappingNode> lastNodes = new LinkedList<>();
        Queue<SchemaMappingNode> iterator = new LinkedList<>();
        if (node.updated) {
            iterator.offer(node);
        }
        while (!iterator.isEmpty()) {
            SchemaMappingNode first = iterator.poll();
            if (first.next == null) {
                lastNodes.add(first);
            } else {
                if (first.next.stream().filter(oneOfNext -> oneOfNext.updated).count() == 0) {
                    lastNodes.add(first);
                    continue;
                }
                for (SchemaMappingNode oneOfNext : first.next) {
                    if (oneOfNext.updated) {
                        iterator.offer(oneOfNext);
                    }
                }
            }
        }
        return lastNodes;
    }

    private static List<SchemaMappingNode> findLastNodesOfChain(SchemaMappingNode node) {
        List<SchemaMappingNode> lastNodes = new LinkedList<>();
        if (node.next == null) {
            lastNodes.add(node);
            return lastNodes;
        }

        Queue<SchemaMappingNode> iterator = new LinkedList<>();
        iterator.offer(node);
        while (!iterator.isEmpty()) {
            SchemaMappingNode first = iterator.poll();
            if (first.next == null) {
                lastNodes.add(first);
            } else {
                for (SchemaMappingNode oneOfNext : first.next) {
                    iterator.offer(oneOfNext);
                }
            }
        }
        return lastNodes;
    }

    public class SchemaMappingNode {

        private Attribute attribute;
        private List<SchemaMappingNode> next;

        // actual data starts from layer 1, layer 0 is preserved for only the root node.
        private int layer = 1;

        private boolean updated = false;

        public SchemaMappingNode(Attribute attribute) {
            this.attribute = attribute;
        }

        public SchemaMappingNode(Attribute attribute, int layer) {
            this(attribute);
            this.layer = layer;
        }

        public Attribute getAttribute() {
            return attribute;
        }

        public int getLayer() {
            return layer;
        }

        public void update() {
            this.updated = true;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SchemaMappingNode that = (SchemaMappingNode) o;
            return layer == that.layer &&
                    Objects.equals(attribute, that.attribute);
        }

        @Override
        public int hashCode() {
            return Objects.hash(attribute, layer);
        }
    }
}
