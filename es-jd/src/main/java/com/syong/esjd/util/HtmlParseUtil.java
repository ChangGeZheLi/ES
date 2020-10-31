package com.syong.esjd.util;

import com.syong.esjd.pojo.Content;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description: 解析网页
 */
@Component
public class HtmlParseUtil {

    public List<Content> parseJD(String keyword) throws Exception {
        /**
         * 获取请求 https://search.jd.com/Search?keyword=java
         **/

        ArrayList<Content> goodsList = new ArrayList<>();

        //根据传入的关键字动态搜索,因为没有转义，所以中文不能查询
        String url = "https://search.jd.com/Search?keyword=" + keyword;
        //Jsoup返回Document就是浏览器中Document对象
        Document document = Jsoup.parse(new URL(url), 30000);
        //所有在js中的方法，都能使用document来用
        Element element = document.getElementById("J_goodsList");

        //获取元素li下的内容
        Elements liElement = document.getElementsByTag("li");
        //获取li元素中的内容
        for (Element el : liElement) {
            //获取对应元素中的内容
            //获取图片，因为京东使用的是懒加载机制，所以图片真正在data-lazy-img属性下
            String img = el.getElementsByTag("img").eq(0).attr("data-lazy-img");
            String price = el.getElementsByClass("p-price").eq(0).text();
            String title = el.getElementsByClass("p-name").eq(0).text();

            //对获取到的元素封装进对象
            goodsList.add(new Content(title,price,img));
        }

        return goodsList;
    }
}
