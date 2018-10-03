package com.parquet.reader;

import com.parquet.model.ParquetToJavaMapping;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.IteratorItemReader;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

public class CustomParquetReader implements ItemReader<ParquetToJavaMapping> {

    private final String filename;
    private ItemReader<ParquetToJavaMapping> delegate;

    public CustomParquetReader(final String filename) {
        this.filename = filename;
    }

    public CustomParquetReader() {
        this.filename = null;
    }

    @Override
    public ParquetToJavaMapping read() throws Exception {
        if (delegate == null) {
            delegate = new IteratorItemReader<>(parquetToJavaMappings());
        }

        return delegate.read();
    }


    private List<ParquetToJavaMapping> parquetToJavaMappings() throws FileNotFoundException {
        List<ParquetToJavaMapping> readParquetFile = new ArrayList<>();

        ParquetToJavaMapping parquetToJavaMapping = new ParquetToJavaMapping();
        parquetToJavaMapping.setName("Ajay Kumar");
        readParquetFile.add(parquetToJavaMapping);
        System.out.println("I am in Reader");
        return readParquetFile;
    }

}
