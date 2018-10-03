package com.parquet.writer;

import com.parquet.model.JavaToParquetFile;

import java.util.List;

public class CustomParquetWriter implements org.springframework.batch.item.ItemWriter<JavaToParquetFile> {

    @Override
    public void write(List<? extends JavaToParquetFile> list) throws Exception {

        System.out.println("I am in writer" + list.size());

    }
}
