package de.alice.springbatch.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * @author alice_b
 */
@Configuration
public class SampleJob {

  @Bean
  public Job firstJob(JobRepository jobRepository) {
    return new JobBuilder("First Job", ).start()
  }

  private Step firstStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
    return new StepBuilder("First Step", jobRepository)
        .tasklet(firstTask(), transactionManager)
        .build();
  }

  private Tasklet firstTask() {
    return (contribution, chunkContext) -> {
      System.out.println("This is the first tasklet step");
      return RepeatStatus.FINISHED;
    };
  }

}
