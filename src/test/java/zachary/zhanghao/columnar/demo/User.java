/**
 * 
 */
package zachary.zhanghao.columnar.demo;

import lombok.Data;
import zachary.zhanghao.columnar.annotation.Column;
import zachary.zhanghao.columnar.annotation.RowKey;
import zachary.zhanghao.columnar.annotation.Table;

/**
 * @author zachary.zhang
 *
 */
@Table(name = "user")
@Data
public class User {

    @RowKey
    private int id;

    @Column(family = "info")
    private int userId;

    @Column(family = "info", name = "user_name1")
    private String userName;

    @Column(family = "info")
    private long age;
}
