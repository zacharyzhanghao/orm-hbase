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
@Table(name = "student")
@Data
public class Student {

    @RowKey
    private int userId;

    @Column(family = "t")
    private String name;

    @Column(family = "t")
    private int age;

    @Column(family = "t")
    private String teacher;
}
