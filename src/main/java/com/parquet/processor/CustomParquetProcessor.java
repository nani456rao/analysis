package com.parquet.processor;

import com.parquet.model.JavaToParquetFile;
import com.parquet.model.ParquetToJavaMapping;
import org.springframework.batch.item.ItemProcessor;

public class CustomParquetProcessor implements ItemProcessor<ParquetToJavaMapping, JavaToParquetFile> {

    @Override
    public JavaToParquetFile process(ParquetToJavaMapping o) throws Exception {
        JavaToParquetFile javaToParquetFile = new JavaToParquetFile();
        javaToParquetFile.setName("Ajay Kumar");

        System.out.println("I am in Processor");

        //To Do Write logic to make a API Call.
        // Map API response to the Java.

        return javaToParquetFile;
    }
}
