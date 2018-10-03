package com.parquet.config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("com.parquet")
public class ParquetSpringBatchMain  {
    public static void main(String[] args) {
        SpringApplication.run(ParquetSpringBatchMain.class, args);
    }
}