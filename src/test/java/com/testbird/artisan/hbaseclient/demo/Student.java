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
