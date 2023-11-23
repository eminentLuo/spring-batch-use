package com.example.demo.task;

import com.example.demo.task.job.DataBatchJob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TimeTask {

    private final JobLauncher jobLauncher;
    private final DataBatchJob dataBatchJob;


    @Autowired
    public TimeTask(JobLauncher jobLauncher, DataBatchJob dataBatchJob) {
        this.jobLauncher = jobLauncher;
        this.dataBatchJob = dataBatchJob;
    }

    @Scheduled(cron = "0/10 * * * * ?")
    public void runBatch() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        log.info("定时任务执行了...");
        // 在运行一个job的时候需要添加至少一个参数，这个参数最后会被写到batch_job_execution_params表中，

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("timestamp",System.currentTimeMillis())
                .toJobParameters();
        Job job = dataBatchJob.dataHandleJob();
        JobExecution execution = jobLauncher.run(job,jobParameters);
        log.info("定时任务结束. Exit Status : {}", execution.getStatus());
    }
}
