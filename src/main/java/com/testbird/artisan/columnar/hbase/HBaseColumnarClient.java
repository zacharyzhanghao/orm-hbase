/**
 * 
 */
package com.testbird.artisan.columnar.hbase;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.testbird.artisan.columnar.annotation.Column;
import com.testbird.artisan.columnar.exception.ColumnarClientException;
import com.testbird.artisan.columnar.funciton.base.Aggregator;
import com.testbird.artisan.columnar.funciton.base.Persistent;
import com.testbird.artisan.columnar.funciton.base.HBaseSourceAware;
import com.testbird.artisan.columnar.utils.HBaseUtils;

/**
 * @author zachary.zhang
 *
 */
@Slf4j
public class HBaseColumnarClient implements Aggregator, Persistent, HBaseSourceAware {

    private static final int PAGE_SIZE_NO_LIMIT = -1;
    private HBaseSource hbaseSource;
    private int scanCaching = 100;// default cache in scan
    private int scanBatch = 100;// default batch in scan

    public HBaseColumnarClient() {}

    /**
     * 设置scan的caching和batch
     * 
     * @param scanCaching 默认值是100
     * @param scanBatch 默认值是100
     */
    public HBaseColumnarClient(int scanCaching, int scanBatch) {
        this.scanCaching = scanCaching;
        this.scanBatch = scanBatch;
    }

    @Override
    public long count(byte[] startRow, byte[] endRow, Class<?> po) throws ColumnarClientException {
        return count(startRow, endRow, po, new Filter[] {});
    }

    @Override
    public long count(byte[] startRow, byte[] endRow, Class<?> po, Filter... filter)
                    throws ColumnarClientException {
        try {
            Scan scan = constructScan(startRow, endRow, filter);

            LongColumnInterpreter columnInterpreter = new LongColumnInterpreter();
            AggregationClient aggregationClient = aggregationClient();

            // No need to close HTable,the AggregationClient close it.
            return aggregationClient.rowCount(TableName.valueOf(HBaseUtils.findTableName(po)),
                            columnInterpreter, scan);
        } catch (Throwable e) {
            log.error("some error occured in count method:", e);
            throw new ColumnarClientException(e);
        }
    }

    @Override
    public long sum(byte[] startRow, byte[] endRow, Class<?> po, String propertyName)
                    throws ColumnarClientException {
        return sum(startRow, endRow, po, propertyName, new Filter[] {});
    }

    @Override
    public long sum(byte[] startRow, byte[] endRow, Class<?> po, String propertyName,
                    Filter... filter) throws ColumnarClientException {
        try {
            Scan scan = constructScan(startRow, endRow, filter);
            AggregationClient aggregationClient = aggregationClient();
            LongColumnInterpreter columnInterpreter = new LongColumnInterpreter();

            indicateColumnToAggregate(po, propertyName, scan);

            // No need to close HTable,the AggregationClient close it.
            return aggregationClient.sum(TableName.valueOf(HBaseUtils.findTableName(po)),
                            columnInterpreter, scan);
        } catch (Throwable e) {
            log.error("some error occured in countAndSum method:", e);
            throw new ColumnarClientException(e);
        }
    }

    @Override
    public void putObject(Object po) throws ColumnarClientException {
        Preconditions.checkNotNull(po, "persistent object can't be null");

        // auto close the resources
        try (HTableInterface table =
                        this.hbaseSource.getTable(HBaseUtils.findTableName(po.getClass()))) {

            Put put = HBaseUtils.wrapPut(po);
            table.put(put);
        } catch (Exception e) {
            log.error("some error occured in putObject method:", e);
            throw new ColumnarClientException(e);
        }
    }

    @Override
    public <T> void putObjectList(List<T> poList) throws ColumnarClientException {
        Preconditions.checkNotNull(poList, "persistent object list can't be null");
        Preconditions.checkArgument(poList.size() != 0, "persistent object list can't be empty");

        try (HTableInterface table =
                        this.hbaseSource.getTable(HBaseUtils
                                        .findTableName(poList.get(0).getClass()))) {

            List<Put> puts = HBaseUtils.wrapPutList(poList);
            table.put(puts);
        } catch (Exception e) {
            log.error("some error occured in putObjectList method:", e);
            throw new ColumnarClientException(e);
        }
    }


    @Override
    public <T> T findObject(byte[] rowKey, Class<T> type) throws ColumnarClientException {
        Preconditions.checkNotNull(rowKey, "rowKey can't be null");

        try (HTableInterface table = this.hbaseSource.getTable(HBaseUtils.findTableName(type))) {

            Get get = new Get(rowKey);
            Result result = table.get(get);
            return HBaseUtils.wrapResult(type, result);
        } catch (Exception e) {
            log.error("some error occured in findObject method:", e);
            throw new ColumnarClientException(e);
        }
    }


    @Override
    public <T> List<T> findObjectList(List<byte[]> rowKeyList, Class<T> type)
                    throws ColumnarClientException {
        Preconditions.checkNotNull(rowKeyList, "rowKey list can't be null");
        Preconditions.checkArgument(rowKeyList.size() != 0, "rowKey  list can't be empty");

        try (HTableInterface table = this.hbaseSource.getTable(HBaseUtils.findTableName(type))) {

            List<Get> gets = Lists.newArrayList();
            for (byte[] rowKey : rowKeyList) {

                Get get = new Get(rowKey);
                gets.add(get);

            }

            Result[] results = table.get(gets);

            return HBaseUtils.wrapResultList(type, results);
        } catch (Exception e) {
            log.error("some error occured in findObjectList method:", e);
            throw new ColumnarClientException(e);
        }
    }

    @Override
    public <T> List<T> findObjectList(byte[] startRow, byte[] endRow, Class<T> type)
                    throws ColumnarClientException {
        Preconditions.checkNotNull(type, "class type can't be null");

        try (HTableInterface table = this.hbaseSource.getTable(HBaseUtils.findTableName(type));) {

            Scan scan = constructScan(startRow, endRow);

            List<T> resultList = Lists.newArrayList();
            ResultScanner scanner = table.getScanner(scan);

            Result result = null;
            while ((result = scanner.next()) != null) {
                T wrapResult = HBaseUtils.wrapResult(type, result);
                resultList.add(wrapResult);
            }

            return resultList;
        } catch (Exception e) {
            log.error("some error occured in findObjectList method:", e);
            throw new ColumnarClientException(e);
        }
    }



    @Override
    public void deleteObject(byte[] rowKey, Class<?> po) throws ColumnarClientException {
        Preconditions.checkNotNull(po, "persistent po class can't be null");

        try (HTableInterface table = hbaseSource.getTable(HBaseUtils.findTableName(po))) {

            table.delete(HBaseUtils.wrapDelete(rowKey));
        } catch (Exception e) {
            log.error("some error occured in deleteObject method:", e);
            throw new ColumnarClientException(e);
        }
    }

    @Override
    public void deleteObjectList(List<byte[]> rowKeyList, Class<?> po)
                    throws ColumnarClientException {
        Preconditions.checkNotNull(po, "persistent po class can't be null");

        try (HTableInterface table = this.hbaseSource.getTable(HBaseUtils.findTableName(po))) {

            List<Delete> deleteList = Lists.newArrayList();
            for (byte[] rowKey : rowKeyList) {
                deleteList.add(HBaseUtils.wrapDelete(rowKey));
            }
            table.delete(deleteList);
        } catch (Exception e) {
            log.error("some error occured in deleteObjectList method:", e);
            throw new ColumnarClientException(e);
        }
    }

    @Override
    public HBaseSource getHBaseSource() {
        return this.hbaseSource;
    }

    @Override
    public void setHBaseSource(HBaseSource hbaseSource) {
        this.hbaseSource = hbaseSource;
    }

    /**
     * get AggregationClient
     * 
     * @return
     */
    protected AggregationClient aggregationClient() {
        AggregationClient aggregationClient =
                        new AggregationClient(this.hbaseSource.getConfiguration());
        return aggregationClient;
    }



    /**
     * 指定column作为聚合计算
     * 
     * @param po
     * @param propertyName
     * @param scan
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws ColumnarClientException
     */
    private void indicateColumnToAggregate(Class<?> po, String propertyName, Scan scan)
                    throws NoSuchFieldException, SecurityException, ColumnarClientException {
        Field sumField = po.getDeclaredField(propertyName);
        if (!sumField.isAnnotationPresent(Column.class)) {
            throw new ColumnarClientException("field :[" + propertyName
                            + "] didn't mark HBaseColumn annotation");
        }
        Column columnSchema = sumField.getAnnotation(Column.class);

        String cn = "";
        if (StringUtils.isBlank(columnSchema.name())) {
            cn = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, propertyName);
        } else {
            cn = columnSchema.name();
        }

        scan.addColumn(Bytes.toBytes(columnSchema.family()), Bytes.toBytes(cn));
    }

    @Override
    public <T> PageBean<T> findObjectListByPage(PageBean<T> pageBean)
                    throws ColumnarClientException {
        return findObjectListByPage(pageBean, new Filter[] {});
    }

    @Override
    public <T> PageBean<T> findObjectListByPage(PageBean<T> pageBean, Filter... filters)
                    throws ColumnarClientException {

        Preconditions.checkNotNull(pageBean.getStartRow(), "start row can't be empty");
        Preconditions.checkNotNull(pageBean.getStopRow(), "end row can't be empty");
        Preconditions.checkArgument(pageBean.getPageSize() > 0, "page size must greater than 0");

        Scan scan =
                        constructScan(pageBean.getStartRow(), pageBean.getStopRow(),
                                        pageBean.getPageSize(), filters);

        List<T> resultList = Lists.newArrayList();
        try (HTableInterface table =
                        this.hbaseSource.getTable(HBaseUtils.findTableName(pageBean
                                        .getGenericType()))) {

            ResultScanner scanner = table.getScanner(scan);

            Result result = null;
            while ((result = scanner.next()) != null) {

                T wrapResult = HBaseUtils.wrapResult(pageBean.getGenericType(), result);
                resultList.add(wrapResult);
            }

            pageBean.setDataSet(resultList);
            if (resultList != null && resultList.size() > 0) {

                // 取到最后一条数据的rowkey作为新的startRow
                T t = resultList.get(resultList.size() - 1);


                // make startRow exclusive add a trailing 0 byte
                pageBean.setStartRow(Bytes.add(HBaseUtils.getRowKeyFromPo(t), new byte[] {0}));
            }
            pageBean.setCurrentPage(pageBean.getCurrentPage() + 1);

        } catch (Exception e) {
            log.error("some error occured in deleteObjectList method:", e);
            throw new ColumnarClientException(e);
        }
        return pageBean;
    }



    private Scan constructScan(byte[] startRow, byte[] endRow, Filter... filters) {
        return constructScan(startRow, endRow, PAGE_SIZE_NO_LIMIT, filters);
    }


    /**
     * @param startRow
     * @param endRow
     * @param pageSize
     * @param filters
     * @return
     */
    private Scan constructScan(byte[] startRow, byte[] endRow, int pageSize, Filter... filters) {
        Scan scan = new Scan();
        if (startRow != null) {
            scan.setStartRow(startRow);
        }

        if (endRow != null) {
            scan.setStopRow(endRow);
        }

        List<Filter> filterList = Lists.newArrayList();
        if (pageSize != PAGE_SIZE_NO_LIMIT) {
            filterList.add(new PageFilter(pageSize));
        }

        if (filters != null && filters.length > 0) {
            filterList.addAll(Arrays.asList(filters));
        }

        if (filterList.size() > 0) {
            FilterList f = new FilterList(filterList.toArray(new Filter[filterList.size()]));
            scan.setFilter(f);
        }

        scan.setCaching(scanCaching);
        // scan.setBatch(scanBatch);
        return scan;
    }
}
