package de.alice.springbatch.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Arrays;
import java.util.List;

/**
 * @author alice_b
 */
@Configuration
public class BatchConfigSystemOut {

    public static final Logger logger = LoggerFactory.getLogger(BatchConfigSystemOut.class);

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private static final int BATCH_SIZE = 5;

    public BatchConfigSystemOut(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
    }

    @Bean
    public Job systemOutJob() {
        return new JobBuilder("First Job", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(taskletStep())
                .next(chunkStep())
                .build();
    }

    @Bean
    public Step taskletStep() {
        return new StepBuilder("First Step", jobRepository)
                .tasklet((stepContribution, chunkContext) -> {
                    logger.info("This is first tasklet step");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step chunkStep() {
        return new StepBuilder("First Step", jobRepository)
                .<String, String>chunk(BATCH_SIZE, transactionManager)
                .reader(reader())
                .writer(writer())
                .build();
    }

    @Bean
    public ItemReader<String> reader() {
        List<String> data = Arrays.asList("foo", "bar", "baz");
        return new ListItemReader<>(data);
    }

    @Bean
    public ItemWriter<String> writer() {
        return items -> {
            for (String item : items) {
                logger.info("Writing item: {}", item);
            }
        };
    }

}
