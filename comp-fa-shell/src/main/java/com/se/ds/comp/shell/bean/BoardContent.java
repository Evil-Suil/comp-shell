package com.se.ds.comp.shell.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * @Author: Yelu Gu
 * @Date: 10/18/2023 2:45 PM
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class BoardContent {
    private String id;
    private String content;
}
