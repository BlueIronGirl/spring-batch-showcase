package de.alice.springbatch.config.article;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

public class ArticleListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        System.out.println("Before Job");
        System.out.println("Job started at: " + jobExecution.getStartTime());
        System.out.println("Status of the Job: " + jobExecution.getStatus());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        System.out.println("After Job");
        System.out.println("Job ended at: " + jobExecution.getEndTime());
        System.out.println("Status of the Job: " + jobExecution.getStatus());
    }
}
