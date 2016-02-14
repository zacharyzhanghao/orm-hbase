/**
 * 
 */
package zachary.zhanghao.columnar.hbase;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;

import org.apache.hadoop.hbase.filter.Filter;

import zachary.zhanghao.columnar.exception.ColumnarClientException;
import zachary.zhanghao.columnar.funciton.ColumnarClient;

import com.google.common.base.Preconditions;

/**
 * supply Criterias function to operate data(CRUD),like JPA or Hibernate QBC
 * 
 * @author zachary.zhang
 *
 */
public class Criterias {


    private Operation<?> op;

    public Criterias(Operation<?> op) {
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

    public <T> void excute(ColumnarClient client) throws ColumnarClientException {
        if (op instanceof PutOp) {
            @SuppressWarnings("unchecked")
            PutOp<T> putOp = (PutOp<T>) op;

            if (putOp.getPoList() != null) {
                client.putObjectList(putOp.getPoList());
            } else {
                client.putObject(putOp.getPo());
            }
        } else if (op instanceof DeleteOp) {
            @SuppressWarnings("unchecked")
            DeleteOp<T> deleteOp = (DeleteOp<T>) op;

            if (deleteOp.getRowKeyList() != null) {
                client.deleteObjectList(deleteOp.getRowKeyList(), deleteOp.getPoClass());
            } else {
                client.deleteObject(deleteOp.getRowKey(), deleteOp.getPoClass());
            }
        } else {
            throw new ColumnarClientException("operation can't match");
        }
    }

    public <T> T query(ColumnarClient client) throws ColumnarClientException {
        if (op instanceof FindOp) {
            @SuppressWarnings("unchecked")
            FindOp<T> findOp = (FindOp<T>) op;

            if (findOp.getRowKey() != null) {
                return client.findObject(findOp.getRowKey(), findOp.getPoClass());
            }
        }
        throw new ColumnarClientException("operation can't match");
    }

    public <T> List<T> queryList(ColumnarClient client) throws ColumnarClientException {
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

    public <T> PageBean<T> queryPage(ColumnarClient client) throws ColumnarClientException {
        if (op instanceof FindOp) {
            @SuppressWarnings("unchecked")
            FindOp<T> findOp = (FindOp<T>) op;

            if (findOp.getPageBean() != null) {
                return client.findObjectListByPage(findOp.getPageBean(), findOp.getFilter());
            }
            return findOp.getPageBean();
        }
        throw new ColumnarClientException("operation can't match");
    }

    public <T> long count(ColumnarClient client) throws ColumnarClientException {
        if (op instanceof AggregateOp) {
            @SuppressWarnings("unchecked")
            AggregateOp<T> aggregateOp = (AggregateOp<T>) op;

            return client.count(aggregateOp.getStartRow(), aggregateOp.getEndRow(),
                            aggregateOp.getPoClass(), aggregateOp.getFilters());
        }
        throw new ColumnarClientException("operation can't match");
    }


    public <T> long sum(ColumnarClient client) throws ColumnarClientException {
        if (op instanceof AggregateOp) {
            @SuppressWarnings("unchecked")
            AggregateOp<T> aggregateOp = (AggregateOp<T>) op;

            return client.countAndSum(aggregateOp.getStartRow(), aggregateOp.getEndRow(),
                            aggregateOp.getPoClass(), aggregateOp.getPropertyName(),
                            aggregateOp.getFilters());
        }
        throw new ColumnarClientException("operation can't match");
    }

    private static interface Operation<T> {
        Class<?> getPoClass();
    }

    @AllArgsConstructor
    @Getter
    private static class FindOp<T> implements Operation<T> {
        private Class<T> poClass;

        private byte[] rowKey;

        private List<byte[]> rowKeyList;

        private byte[] startRow;

        private byte[] endRow;

        private Filter[] filter;

        private PageBean<T> pageBean;
    }

    @AllArgsConstructor
    @Getter
    private static class PutOp<T> implements Operation<T> {


        private Class<T> poClass;

        private T po;

        private List<T> poList;

    }

    @AllArgsConstructor
    @Getter
    private static class DeleteOp<T> implements Operation<T> {

        private Class<T> poClass;

        private byte[] rowKey;

        private List<byte[]> rowKeyList;

    }

    @AllArgsConstructor
    @Getter
    private static class AggregateOp<T> implements Operation<T> {


        private Class<T> poClass;

        private byte[] startRow;

        private byte[] endRow;

        private Filter[] filters;

        private String propertyName;
    }

    public static class CriteriasFindBuilder<T> {

        private Class<T> poClass;

        private byte[] rowKey;

        private List<byte[]> rowKeyList;

        private byte[] startRow;

        private byte[] endRow;

        private Filter[] filters;

        private PageBean<T> pageBean;

        public CriteriasFindBuilder(Class<T> poClass) {
            this.poClass = poClass;
        }

        public CriteriasFindBuilder<T> byRowKey(byte[] rowKey) {
            this.rowKey = rowKey;
            return this;
        }

        public CriteriasFindBuilder<T> byRowKeyList(List<byte[]> rowKeyList) {
            this.rowKeyList = rowKeyList;
            return this;
        }

        public CriteriasFindBuilder<T> startRow(byte[] startRow) {
            this.startRow = startRow;
            return this;
        }

        public CriteriasFindBuilder<T> endRow(byte[] endRow) {
            this.endRow = endRow;
            return this;
        }

        public CriteriasFindBuilder<T> filters(Filter[] filters) {
            this.filters = filters;
            return this;
        }

        public CriteriasFindBuilder<T> pageBean(PageBean<T> pageBean) {
            this.pageBean = pageBean;
            return this;
        }

        public Criterias build() {
            FindOp<T> op =
                            new FindOp<T>(poClass, rowKey, rowKeyList, startRow, endRow, filters,
                                            pageBean);
            return new Criterias(op);
        }
    }

    public static class CriteriasPutBuilder<T> {
        private Class<T> poClass;

        private T po;

        private List<T> poList;

        public CriteriasPutBuilder(Class<T> poClass) {
            this.poClass = poClass;
        }

        public CriteriasPutBuilder<T> persistenObject(T po) {
            this.po = po;
            return this;
        }

        public CriteriasPutBuilder<T> persistenObjectList(List<T> poList) {
            this.poList = poList;
            return this;
        }

        public Criterias build() {
            PutOp<T> putOp = new PutOp<T>(poClass, po, poList);
            return new Criterias(putOp);
        }
    }

    public static class CriteriasDeleteBuilder<T> {
        private Class<T> poClass;

        private byte[] rowKey;

        private List<byte[]> rowKeyList;

        public CriteriasDeleteBuilder(Class<T> poClass) {
            this.poClass = poClass;
        }

        public CriteriasDeleteBuilder<T> byRowKey(byte[] rowKey) {
            this.rowKey = rowKey;
            return this;
        }

        public CriteriasDeleteBuilder<T> byRowKeyList(List<byte[]> rowKeyList) {
            this.rowKeyList = rowKeyList;
            return this;
        }

        public Criterias build() {
            DeleteOp<T> op = new DeleteOp<T>(poClass, rowKey, rowKeyList);
            return new Criterias(op);
        }
    }

    public static class CriteriasAggregateBuilder<T> {

        private Class<T> poClass;

        private byte[] startRow;

        private byte[] endRow;

        private Filter[] filters;

        private String propertyName;

        public CriteriasAggregateBuilder(Class<T> poClass) {
            this.poClass = poClass;
        }

        public CriteriasAggregateBuilder<T> startRow(byte[] startRow) {
            this.startRow = startRow;
            return this;
        }

        public CriteriasAggregateBuilder<T> endRow(byte[] endRow) {
            this.endRow = endRow;
            return this;
        }

        public CriteriasAggregateBuilder<T> filters(Filter[] filters) {
            this.filters = filters;
            return this;
        }

        public CriteriasAggregateBuilder<T> propertyName(String propertyName) {
            this.propertyName = propertyName;
            return this;
        }

        public Criterias build() {
            AggregateOp<T> op =
                            new AggregateOp<T>(poClass, startRow, endRow, filters, propertyName);
            return new Criterias(op);
        }
    }

}
