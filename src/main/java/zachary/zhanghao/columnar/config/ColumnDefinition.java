/**
 * 
 */
package zachary.zhanghao.columnar.config;

import java.lang.reflect.Field;

import lombok.Data;

import org.apache.commons.lang.StringUtils;

import zachary.zhanghao.columnar.annotation.Column;
import zachary.zhanghao.columnar.annotation.RowKey;

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;

/**
 * wrap column definition for schema
 * 
 * @author zachary.zhang
 *
 */
@Data
public class ColumnDefinition {

    private String columnFamily;
    private String columnName;
    private long timeStamp;
    private Field field;
    private Field rowKey;
    private String hbaseColumnName;
    private Class<?> fieldType;


    public static ColumnDefinition parse(Field field) {
        if (field.isAnnotationPresent(RowKey.class)) {
            ColumnDefinition colDef = new ColumnDefinition();
            colDef.setFieldType(field.getType());
            colDef.setRowKey(field);
            return colDef;

        } else if (field.isAnnotationPresent(Column.class)) {

            Column hbaseColumn = field.getAnnotation(Column.class);
            Preconditions.checkState(StringUtils.isNotBlank(hbaseColumn.family()),
                            "hbase column family can't be null");


            ColumnDefinition colDef = new ColumnDefinition();
            colDef.setColumnFamily(hbaseColumn.family());
            colDef.setFieldType(field.getType());
            colDef.setColumnName(field.getName());
            colDef.setField(field);

            // 如果注解中没有定义columnName，则使用property name作为默认值
            if (StringUtils.isBlank(hbaseColumn.name())) {

                // 驼峰式命名转化成小写下划线命名，如：xxYy ---> xx_yy
                colDef.setHbaseColumnName(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE,
                                field.getName()));
            } else {

                colDef.setHbaseColumnName(hbaseColumn.name());
            }
            return colDef;

        }
        // 没有注解的列，不用做映射
        return null;
    }
}
