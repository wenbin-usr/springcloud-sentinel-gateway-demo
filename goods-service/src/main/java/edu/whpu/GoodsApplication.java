package edu.whpu;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Hello world!
 *
 */
@SpringBootApplication
@MapperScan("edu.whpu.mapper")
public class GoodsApplication {
    public static void main( String[] args ) {
        SpringApplication.run(GoodsApplication.class, args);
    }
}

