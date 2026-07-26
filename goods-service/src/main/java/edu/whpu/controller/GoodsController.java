package edu.whpu.controller;

import edu.whpu.entity.Goods;
import edu.whpu.mapper.GoodsMapper;
//import edu.whpu.service.DemoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author bin_wen
 * @date 2025/2/7 00:18
 */
@RestController
@RequestMapping("/goods")
public class GoodsController {

    @Autowired
    private GoodsMapper goodsMapper;

//    @Autowired
//    private DemoService demoService;

    @GetMapping("/list")
    public List<Goods> list() throws InterruptedException {
//        demoService.demo1();
//        demoService.demo2();
//        Thread.sleep(6000);
        return goodsMapper.selectList(null);
    }

    @PostMapping("/save")
    public Boolean save(@RequestBody Goods goods) {
        return goodsMapper.insert(goods) > 0;
    }

    @GetMapping("/{id}")
    public Goods getById(@PathVariable Long id) {
        return goodsMapper.selectById(id);
    }


}
