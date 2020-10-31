package com.syong.esjd.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * @Description:
 */
@Controller
public class IndexController {

    @GetMapping({"/","index"})
    public String index(){
        return "index";
    }
}
