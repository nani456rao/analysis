package com.parquet.config;


import com.parquet.model.JavaToParquetFile;
import com.parquet.model.ParquetToJavaMapping;
import com.parquet.processor.CustomParquetProcessor;
import com.parquet.reader.CustomParquetReader;
import com.parquet.writer.CustomParquetWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class ParquetJobConfig {


    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Bean
    public Job readParquetJobConfiguration() {
        return jobBuilderFactory.get("parquetReportJob")
                .start(taskletStep())
                .next(chunkStep())
                .build();
    }

    @Bean
    public Step chunkStep() {
        return stepBuilderFactory.get("chunkStep")
                .<ParquetToJavaMapping, JavaToParquetFile>chunk(20)
                .reader(customParquetReader())
                .processor(customParquetProcessor())
                .writer(customParquetWriter())
                .build();
    }

    @Bean
    public Step taskletStep() {
        return stepBuilderFactory.get("taskletStep")
                .tasklet(tasklet())
                .build();
    }

    @Bean
    public Tasklet tasklet() {
        return (contribution, chunkContext) -> {
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    @StepScope
    public CustomParquetReader customParquetReader() {
        return new CustomParquetReader();
    }

    @Bean
    @StepScope
    public CustomParquetProcessor customParquetProcessor() {
        return new CustomParquetProcessor();
    }

    @Bean
    @StepScope
    public CustomParquetWriter customParquetWriter() {
        return new CustomParquetWriter();
    }


    @Bean
    public ResourcelessTransactionManager transactionManager() {
        return new ResourcelessTransactionManager();
    }

    @Bean
    public JobRepository jobRepository(ResourcelessTransactionManager transactionManager) throws Exception {
        MapJobRepositoryFactoryBean mapJobRepositoryFactoryBean = new MapJobRepositoryFactoryBean(transactionManager);
        mapJobRepositoryFactoryBean.setTransactionManager(transactionManager);

        return mapJobRepositoryFactoryBean.getObject();
    }

    @Bean
    public SimpleJobLauncher jobLauncher(JobRepository jobRepository) {
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(jobRepository);
        return simpleJobLauncher;
    }
}