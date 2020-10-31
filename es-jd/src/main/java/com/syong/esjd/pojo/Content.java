package com.syong.esjd.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description: 用于封装爬取的网页内容
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Content {

    private String title;
    private String price;
    private String img;
}
