/**
 * 
 */
package com.testbird.artisan.columnar.hbase;

import java.util.List;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import org.apache.hadoop.hbase.filter.Filter;

import com.google.common.base.Preconditions;
import com.testbird.artisan.columnar.exception.ColumnarClientException;
import com.testbird.artisan.columnar.funciton.base.Aggregator;
import com.testbird.artisan.columnar.funciton.base.Persistent;

/**
 * supply Criterias function to operate data(CRUD),like JPA or Hibernate QBC
 * 
 * @author zachary.zhang
 *
 */
public class Criteria {

    private Operation<?> op;

    public Criteria(Operation<?> op) {
        this.op = op;
    }

    public static <T> CriteriasFindBuilder<T> find(Class<T> poClass) {
        Preconditions.checkNotNull(poClass, "persistent object can't be null");
        return new CriteriasFindBuilder<T>(poClass);
    }

    public static <T> CriteriasDeleteBuilder<T> delete(Class<T> poClass) {
        Preconditions.checkNotNull(poClass, "persistent object can't be null");
        return new CriteriasDeleteBuilder<T>(poClass);
    }

    public static <T> CriteriasPutBuilder<T> put(Class<T> poClass) {
        Preconditions.checkNotNull(poClass, "persistent object can't be null");
        return new CriteriasPutBuilder<T>(poClass);
    }

    public static <T> CriteriasAggregateBuilder<T> aggregate(Class<T> poClass) {
        Preconditions.checkNotNull(poClass, "persistent object can't be null");
        return new CriteriasAggregateBuilder<T>(poClass);
    }

    /**
     * excute the command: put/delete
     * 
     * @param client
     * @throws ColumnarClientException
     */
    public void excute(Persistent client) throws ColumnarClientException {
        if (op instanceof PutOp) {
            PutOp<?> putOp = (PutOp<?>) op;

            if (putOp.getPoList() != null) {
                client.putObjectList(putOp.getPoList());
            } else {
                client.putObject(putOp.getPo());
            }
        } else if (op instanceof DeleteOp) {
            DeleteOp<?> deleteOp = (DeleteOp<?>) op;

            if (deleteOp.getRowKeyList() != null) {
                client.deleteObjectList(deleteOp.getRowKeyList(), deleteOp.getPoClass());
            } else {
                client.deleteObject(deleteOp.getRowKey(), deleteOp.getPoClass());
            }
        } else {
            throw new ColumnarClientException("operation can't match");
        }
    }

    /**
     * query single data
     * 
     * @param client
     * @return
     * @throws ColumnarClientException
     */
    public <T> T query(Persistent client) throws ColumnarClientException {
        if (op instanceof FindOp) {
            @SuppressWarnings("unchecked")
            FindOp<T> findOp = (FindOp<T>) op;

            if (findOp.getRowKey() != null) {
                return client.findObject(findOp.getRowKey(), findOp.getPoClass());
            }
        }
        throw new ColumnarClientException("operation can't match");
    }

    /**
     * query list data
     * 
     * @param client
     * @return
     * @throws ColumnarClientException
     */
    public <T> List<T> queryList(Persistent client) throws ColumnarClientException {
        if (op instanceof FindOp) {
            @SuppressWarnings("unchecked")
            FindOp<T> findOp = (FindOp<T>) op;

            if (findOp.getRowKeyList() != null) {
                return client.findObjectList(findOp.getRowKeyList(), findOp.getPoClass());
            } else if (findOp.getStartRow() != null) {
                return client.findObjectList(findOp.getStartRow(), findOp.getEndRow(),
                                findOp.getPoClass());
            }
        }
        throw new ColumnarClientException("operation can't match");
    }

    /**
     * query by page(pagination)
     * 
     * @param client
     * @return
     * @throws ColumnarClientException
     */
    public <T> PageBean<T> queryPage(Persistent client) throws ColumnarClientException {
        if (op instanceof FindOp) {
            @SuppressWarnings("unchecked")
            FindOp<T> findOp = (FindOp<T>) op;

            if (findOp.getPageBean() != null) {
                return client.findObjectListByPage(findOp.getPageBean(), findOp.getFilters());
            }
            return findOp.getPageBean();
        }
        throw new ColumnarClientException("operation can't match");
    }

    /**
     * count the data row
     * 
     * @param client
     * @return
     * @throws ColumnarClientException
     */
    public long count(Aggregator client) throws ColumnarClientException {
        if (op instanceof AggregateOp) {
            AggregateOp<?> aggregateOp = (AggregateOp<?>) op;

            return client.count(aggregateOp.getStartRow(), aggregateOp.getEndRow(),
                            aggregateOp.getPoClass(), aggregateOp.getFilters());
        }
        throw new ColumnarClientException("operation can't match");
    }

    /**
     * sum the data value,only can support Num
     * 
     * @param client
     * @return
     * @throws ColumnarClientException
     */
    public long sum(Aggregator client) throws ColumnarClientException {
        if (op instanceof AggregateOp) {
            AggregateOp<?> aggregateOp = (AggregateOp<?>) op;

            return client.sum(aggregateOp.getStartRow(), aggregateOp.getEndRow(),
                            aggregateOp.getPoClass(), aggregateOp.getPropertyName(),
                            aggregateOp.getFilters());
        }
        throw new ColumnarClientException("operation can't match");
    }

    /**
     * operation interface
     * 
     * @author zachary.zhang
     *
     * @param <T>
     */
    protected interface Operation<T> {
        Class<T> getPoClass();
    }

    @NoArgsConstructor
    @Setter
    @Getter
    protected static class FindOp<T> implements Operation<T> {
        private Class<T> poClass;

        private byte[] rowKey;

        private List<byte[]> rowKeyList;

        private byte[] startRow;

        private byte[] endRow;

        private Filter[] filters;

        private PageBean<T> pageBean;
    }

    @NoArgsConstructor
    @Setter
    @Getter
    protected static class PutOp<T> implements Operation<T> {

        private Class<T> poClass;

        private T po;

        private List<T> poList;

    }
    @NoArgsConstructor
    @Setter
    @Getter
    protected static class DeleteOp<T> implements Operation<T> {

        private Class<T> poClass;

        private byte[] rowKey;

        private List<byte[]> rowKeyList;

    }

    @NoArgsConstructor
    @Setter
    @Getter
    protected static class AggregateOp<T> implements Operation<T> {

        private Class<T> poClass;

        private byte[] startRow;

        private byte[] endRow;

        private Filter[] filters;

        private String propertyName;
    }

    public static class CriteriasFindBuilder<T> {

        private FindOp<T> findOp;

        public CriteriasFindBuilder(Class<T> poClass) {
            findOp = new FindOp<T>();
            findOp.setPoClass(poClass);
        }

        public CriteriasFindBuilder<T> byRowKey(byte[] rowKey) {
            findOp.setRowKey(rowKey);
            return this;
        }

        public CriteriasFindBuilder<T> byRowKeyList(List<byte[]> rowKeyList) {
            findOp.setRowKeyList(rowKeyList);
            return this;
        }

        public CriteriasFindBuilder<T> fromRow(byte[] startRow) {
            findOp.setStartRow(startRow);
            return this;
        }

        public CriteriasFindBuilder<T> toRow(byte[] endRow) {
            findOp.setEndRow(endRow);
            return this;
        }

        public CriteriasFindBuilder<T> filters(Filter[] filters) {
            findOp.setFilters(filters);
            return this;
        }

        public CriteriasFindBuilder<T> pageBean(PageBean<T> pageBean) {
            findOp.setPageBean(pageBean);
            return this;
        }

        public Criteria build() {
            return new Criteria(findOp);
        }
    }

    public static class CriteriasPutBuilder<T> {

        private PutOp<T> putOp;

        public CriteriasPutBuilder(Class<T> poClass) {
            putOp = new PutOp<T>();
            putOp.setPoClass(poClass);
        }

        public CriteriasPutBuilder<T> persistenObject(T po) {
            putOp.setPo(po);
            return this;
        }

        public CriteriasPutBuilder<T> persistenObjectList(List<T> poList) {
            putOp.setPoList(poList);
            return this;
        }

        public Criteria build() {
            return new Criteria(putOp);
        }
    }

    public static class CriteriasDeleteBuilder<T> {

        private DeleteOp<T> deleteOp;

        public CriteriasDeleteBuilder(Class<T> poClass) {
            deleteOp = new DeleteOp<T>();
            deleteOp.setPoClass(poClass);
        }

        public CriteriasDeleteBuilder<T> byRowKey(byte[] rowKey) {
            deleteOp.setRowKey(rowKey);
            return this;
        }

        public CriteriasDeleteBuilder<T> byRowKeyList(List<byte[]> rowKeyList) {
            deleteOp.setRowKeyList(rowKeyList);
            return this;
        }

        public Criteria build() {
            return new Criteria(deleteOp);
        }
    }

    public static class CriteriasAggregateBuilder<T> {

        private AggregateOp<T> aggregateOp;

        public CriteriasAggregateBuilder(Class<T> poClass) {
            aggregateOp = new AggregateOp<T>();
            aggregateOp.setPoClass(poClass);
        }

        public CriteriasAggregateBuilder<T> fromRow(byte[] startRow) {
            aggregateOp.setStartRow(startRow);
            return this;
        }

        public CriteriasAggregateBuilder<T> toRow(byte[] endRow) {
            aggregateOp.setEndRow(endRow);
            return this;
        }

        public CriteriasAggregateBuilder<T> filters(Filter[] filters) {
            aggregateOp.setFilters(filters);
            return this;
        }

        public CriteriasAggregateBuilder<T> propertyName(String propertyName) {
            aggregateOp.setPropertyName(propertyName);
            return this;
        }

        public Criteria build() {
            return new Criteria(aggregateOp);
        }
    }
}
