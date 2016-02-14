/**
 * 
 */
package zachary.zhanghao.columnar.hbase;

import java.lang.reflect.ParameterizedType;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

import org.codehaus.jackson.type.TypeReference;

/**
 * 分页查询的结果对象
 * 
 * <Pre>
 * this is abstract class,the sub class should extends this class that we can get the generic type.
 * <p> just like use {@link TypeReference} in Jackson
 * 
 * @author zachary.zhang
 *
 */
@Getter
@Setter
public abstract class PageBean<T> {

    /**
     * 返回查询之后获得的数据集
     */
    private List<T> dataSet;


    /**
     * 设定开始查询的startRow.第一次由用户提供，后面通过框架计算生成.
     */
    private byte[] startRow;

    /**
     * 设定结束查询的stopRow
     */
    private byte[] stopRow;


    /**
     * 当前结果集是第几页，方便页面显示.default是0页
     */
    private int currentPage = 0;

    /**
     * 每页显示多少行数据.default是100条数据
     * 
     * <pre>注意：一经设置，中途查询数据的过程中，请勿修改
     */
    private int pageSize = 100;


    @SuppressWarnings("unchecked")
    /**
     * get the generic type T class
     * @return Class
     */
    public Class<T> getGenericType() {
        return (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass())
                        .getActualTypeArguments()[0];
    }

    // TODO should add totalCount in future
    // private int totalCount
}
