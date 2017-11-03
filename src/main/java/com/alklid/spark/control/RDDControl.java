package com.alklid.spark.control;

import com.alklid.spark.service.RDDService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

@RestController
public class RDDControl {

    @Autowired
    private RDDService rddService;

    @RequestMapping(value="/rdd", method= RequestMethod.GET)
    public Object ch2CreateRDDPost(
            @RequestHeader Map<String, String> header
            ,HttpServletRequest request
            ,HttpServletResponse response
    ) throws IOException {
        return rddService.createRDD();
    }
}
