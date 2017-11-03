package com.alklid.spark.control;

import com.alklid.spark.service.WordCountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@RestController
public class WordCountControl {

    @Autowired
    private WordCountService wordCountService;

    @ResponseBody
    @RequestMapping(value="/wordcount", method= RequestMethod.POST)
    public Object ch1WordCountPost(
            @RequestHeader Map<String, String> header
            ,HttpServletRequest request
            ,HttpServletResponse response
            ,@RequestBody HashMap<String, Object> body
    ) throws IOException {
        /*
            body
            - inputFile
            - outputDirPath
        */
        return wordCountService.wordCountPost(body);
    }

}
