package com.example.demo.task.job;

import com.example.demo.model.Student;
import com.example.demo.task.listener.JobListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.orm.JpaNativeQueryProvider;
import org.springframework.stereotype.Component;

import javax.persistence.EntityManagerFactory;

@Slf4j
@Component
public class DataBatchJob {
    //Job构建工厂，用于构建Job
    private final JobBuilderFactory jobBuilderFactory;

    //step构建工厂，用于构建step
    private final StepBuilderFactory stepBuilderFactory;

    //实体类管理工厂，用于访问表格数据
    private final EntityManagerFactory emf;

    //自定义简单Job监听器
    private final JobListener jobListener;


    public DataBatchJob(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, EntityManagerFactory emf, JobListener jobListener) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.emf = emf;
        this.jobListener = jobListener;
    }

    public Job dataHandleJob(){
        return jobBuilderFactory.get("dataHandleJob")
                .incrementer(new RunIdIncrementer())
                //start 是JOB 执行的第一个step
                .start(handleDataStep())
                //调用next方法设置其他step
                //.next()
                //设置自定义的JobListener
                .listener(jobListener)
                .build();
    }

    /**
     * 一个简单基础的Step主要分为三个部分
     * ItemReader : 用于读取数据
     * ItemProcessor : 用于处理数据
     * ItemWriter : 用于写数据
     * @return
     */
    private Step handleDataStep() {
        return stepBuilderFactory.get("getData")
                // <输入对象, 输出对象>  chunk通俗的讲类似于SQL的commit; 这里表示处理(processor)100条后写入(writer)一次
                .<Student,Student>chunk(100)
                // 捕捉到异常就重试,重试100次还是异常,JOB就停止并标志失败
                .faultTolerant().retryLimit(3).retry(Exception.class).skipLimit(100).skip(Exception.class)
                // 指定ItemReader对象
                .reader(getDataReader())
                // 指定ItemProcessor对象
                .processor(getDataProcessor())
                // 指定ItemWriter对象
                .writer(getDataWriter())
                .build();




    }



    //读取数据
    private ItemReader<? extends Student> getDataReader() {
        JpaPagingItemReader<Student> reader = new JpaPagingItemReader<>();

        try {
            JpaNativeQueryProvider<Student> queryProvider = new JpaNativeQueryProvider<>();
            queryProvider.setSqlQuery("SELECT * FROM student");
            queryProvider.setEntityClass(Student.class);
            queryProvider.afterPropertiesSet();

            reader.setEntityManagerFactory(emf);
            reader.setPageSize(3);
            reader.setQueryProvider(queryProvider);
            reader.afterPropertiesSet();

            reader.setSaveState(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return reader;
    }

    //处理数据
    private ItemProcessor<? super Student,? extends Student> getDataProcessor() {

        return student -> {
            // 模拟处理数据，这里处理就是打印一下
            log.info("processor data:  "+ student.toString());
            return student;
        };
    }

    //写入数据
    private ItemWriter<? super Student> getDataWriter() {
        return list -> {
            for (Student student : list) {
                // 模拟写数据，为了演示的简单就不写入数据库了
                log.info("write data : " + student);
            }
        };


    }

}
