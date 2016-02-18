/**
 * 
 */
package com.testbird.artisan.hbaseclient.demo;

import lombok.Data;

import com.testbird.artisan.columnar.annotation.Column;
import com.testbird.artisan.columnar.annotation.RowKey;
import com.testbird.artisan.columnar.annotation.Table;

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
