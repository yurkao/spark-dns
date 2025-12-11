package com.acme.dns.spark.read;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.Option;
import scala.collection.immutable.List; // For empty Seq/List for qualifiers

import java.util.ArrayList;
import java.util.UUID;

/**
 * Helper to convert a Spark {@link StructType} into catalyst attribute references
 * for logical planning within the DNS data source.
 */
public class SchemaConverterFullParams {

    /**
     * Convert every field in the schema into an {@link AttributeReference} preserving metadata.
     * @param schema Spark schema for the DNS dataset
     * @return catalyst attribute sequence used when constructing logical plans
     */
    public static Seq<AttributeReference> convertStructTypeToAttributesFull(StructType schema) {
        java.util.List<AttributeReference> attributes = new ArrayList<>();

        final Seq<String> emptyQualifierSeq = List.empty(); // No specific qualifier needed typically

        for (final StructField field : schema.fields()) {

            // Generate a unique internal expression ID for each attribute
            // This mimics how Spark ensures uniqueness within a plan
            final UUID uuid = UUID.randomUUID();
            final ExprId id = ExprId.apply(uuid.getMostSignificantBits(), uuid);

            // Use the full 6-parameter constructor
            AttributeReference attrRef = new AttributeReference(
                    field.name(),           // 1. name
                    field.dataType(),       // 2. dataType
                    field.nullable(),       // 3. nullable
                    field.metadata(),       // 4. metadata
                    id,                     // 5. exprId
                    emptyQualifierSeq       // 6. qualifier (using an empty Seq/List)
            );
            attributes.add(attrRef);
        }

        // Convert the Java List back to a Scala Seq
        return JavaConverters.asScalaBuffer(attributes).toSeq();
    }

}
