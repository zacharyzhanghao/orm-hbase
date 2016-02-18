/**
 * 
 */
package com.testbird.artisan.columnar.utils;

import java.lang.reflect.Field;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.testbird.artisan.columnar.annotation.Table;
import com.testbird.artisan.columnar.config.ColumnDefinition;
import com.testbird.artisan.columnar.exception.ColumnarClientException;
import com.testbird.artisan.columnar.type.TypeResolver;
import com.testbird.artisan.columnar.type.TypeResolverFactory;

/**
 * hbase utils class , apply some generic functions
 * 
 * @author zachary.zhang
 *
 */
@Slf4j
public class HBaseUtils {
    public static Put wrapPut(Object po) throws ColumnarClientException {

        Preconditions.checkNotNull(po, "persistent object can't be null");

        List<Object> poList = Lists.newArrayList();
        poList.add(po);

        return wrapPutList(poList).get(0);
    }

    public static <T> List<Put> wrapPutList(List<T> poList) throws ColumnarClientException {
        Preconditions.checkNotNull(poList, "persistent object list can't be null");
        Preconditions.checkArgument(poList.size() != 0, "persistent object list can't be empty");


        List<ColumnDefinition> columnInfoList = findColumnInfoList(poList.get(0));

        // acquire the rowkey field
        ColumnDefinition rowKeyCol = extractRowKeyColumn(poList.get(0).getClass(), columnInfoList);

        log.debug("find po column successfully");

        List<Put> puts = Lists.newArrayList();
        try {
            for (Object po : poList) {

                byte[] rowKey =
                                resolveToBytes(rowKeyCol.getFieldType(),
                                                rowKeyCol.getRowKey().get(po));

                // convert value by the schema
                Put put = new Put(rowKey);

                for (ColumnDefinition colDef : columnInfoList) {

                    if (colDef.getField().get(po) == null) {
                        continue;
                    }

                    byte[] fieldValue =
                                    resolveToBytes(colDef.getFieldType(), colDef.getField().get(po));


                    put.add(Bytes.toBytes(colDef.getColumnFamily()),
                                    Bytes.toBytes(colDef.getHbaseColumnName()), fieldValue);
                }
                puts.add(put);
            }
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new ColumnarClientException(e);
        }
        return puts;
    }

    private static ColumnDefinition extractRowKeyColumn(Class<?> poType,
                    List<ColumnDefinition> columnInfoList) throws ColumnarClientException {
        ColumnDefinition rowKeyCol = null;
        for (ColumnDefinition colDef : columnInfoList) {
            if (colDef.getRowKey() != null) {
                rowKeyCol = colDef;
                columnInfoList.remove(colDef);
                break;
            }
        }

        if (rowKeyCol == null) {
            throw new ColumnarClientException("there must a @RowKey annotation in po:"
                            + poType.getName());
        }
        return rowKeyCol;
    }

    @SuppressWarnings("unchecked")
    private static byte[] resolveToBytes(Class<?> fieldType, Object fieldValue)
                    throws IllegalAccessException, ColumnarClientException {
        @SuppressWarnings("rawtypes")
        TypeResolver typeHandler = TypeResolverFactory.getResolver(fieldType);
        if (typeHandler == null) {
            throw new ColumnarClientException("unsupport field type:" + fieldType.getTypeName());
        }

        return typeHandler.toBytes(fieldValue);
    }


    /**
     * 找到column info,返回list
     * 
     * @param po 持久化对象
     * @return List<ColumnInfo>
     */
    private static List<ColumnDefinition> findColumnInfoList(Object po) {
        return findColumnInfoList(po.getClass());
    }

    /**
     * 根据class type找到column info,返回list
     * 
     * @param classType
     * @return
     */
    public static List<ColumnDefinition> findColumnInfoList(Class<?> classType) {

        List<ColumnDefinition> columnInfoList = Lists.newArrayList();

        Field[] fields = classType.getDeclaredFields();

        for (Field field : fields) {
            field.setAccessible(true);

            ColumnDefinition columnInfo = ColumnDefinition.parse(field);
            // 过滤掉没有注解的列
            if (columnInfo == null) {
                continue;
            }

            columnInfoList.add(columnInfo);

        }
        return columnInfoList;
    }

    public static Get wrapGet(byte[] rowKey) {
        Preconditions.checkNotNull(rowKey, "rowKey can't be null");
        return new Get(rowKey);
    }

    public static Delete wrapDelete(byte[] rowKey) {
        Preconditions.checkNotNull(rowKey, "rowKey can't be null");
        return new Delete(rowKey);
    }

    public static <T> T wrapResult(Class<T> type, Result result) throws ColumnarClientException {
        Result[] results = {result};
        List<T> resultList = wrapResultList(type, results);
        return resultList.size() > 0 ? resultList.get(0) : null;
    }

    public static <T> List<T> wrapResultList(Class<T> type, Result[] results)
                    throws ColumnarClientException {
        try {
            List<ColumnDefinition> columnInfoList = findColumnInfoList(type);

            ColumnDefinition rowKeyColumn = extractRowKeyColumn(type, columnInfoList);

            List<T> returList = Lists.newArrayList();
            for (Result result : results) {

                T target = type.newInstance();

                Cell[] cells = result.rawCells();
                if (cells == null || cells.length == 0) {
                    continue;
                }

                // set rowkey value
                TypeResolver<?> rowKeyResolver =
                                TypeResolverFactory.getResolver(rowKeyColumn.getFieldType());
                rowKeyColumn.getRowKey().set(target, rowKeyResolver.toObject(result.getRow()));


                for (ColumnDefinition colDef : columnInfoList) {

                    Cell cell =
                                    result.getColumnLatestCell(
                                                    Bytes.toBytes(colDef.getColumnFamily()),
                                                    Bytes.toBytes(colDef.getColumnName()));

                    if (cell == null) {
                        continue;
                    }

                    TypeResolver<?> typeResolver =
                                    TypeResolverFactory.getResolver(colDef.getFieldType());
                    colDef.getField().set(target, typeResolver.toObject(CellUtil.cloneValue(cell)));
                }

                returList.add(target);
            }

            return returList;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new ColumnarClientException(e);
        }
    }

    public static String findTableName(Class<?> po) {
        Preconditions.checkNotNull(po, "persistent object can't be null");
        Preconditions.checkState(po.isAnnotationPresent(Table.class),
                        "persistent object must have the HBaseTable annotation");


        Table hbaseTable = po.getAnnotation(Table.class);
        String tableName = hbaseTable.name();

        Preconditions.checkState(StringUtils.isNotBlank(tableName), "tableName can't be empty");
        return tableName;
    }

    public static byte[] getRowKeyFromPo(Object po) throws ColumnarClientException {

        List<ColumnDefinition> columnInfoList = findColumnInfoList(po.getClass());

        ColumnDefinition rowKeyColumn = extractRowKeyColumn(po.getClass(), columnInfoList);
        try {
            Object object = rowKeyColumn.getRowKey().get(po);

            return resolveToBytes(rowKeyColumn.getFieldType(), object);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new ColumnarClientException(e);
        }
    }
}
