package de.alice.springbatch.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/job")
public class JobController {
    private final JobLauncher jobLauncher;
    private final ApplicationContext context;

    public JobController(JobLauncher jobLauncher, ApplicationContext context) {
        this.jobLauncher = jobLauncher;
        this.context = context;
    }

    @PostMapping("/{jobName}")
    public void invokeJob(@PathVariable String jobName, JobParameters jobParameters) throws Exception {
        Job jobToStart = context.getBean(jobName, Job.class);
        jobLauncher.run(jobToStart, jobParameters);
    }
}
