package de.alice.springbatch.config.csv;

import de.alice.springbatch.entity.Article;
import de.alice.springbatch.repository.ArticleRepository;
import de.alice.springbatch.util.BlankLineRecordSeparatorPolicy;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class ArticleCSVToDBBatchConfig {

    private final ArticleRepository articleRepository;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private static final int BATCH_SIZE = 5;

    @Autowired
    public ArticleCSVToDBBatchConfig(ArticleRepository articleRepository, JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        this.articleRepository = articleRepository;
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
    }

    @Bean
    public FlatFileItemReader<Article> articleCSVReader() {
        FlatFileItemReader<Article> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("articles.csv"));
        reader.setLineMapper(defaultLineMapper());
        reader.setRecordSeparatorPolicy(new BlankLineRecordSeparatorPolicy());
        return reader;
    }

    private DefaultLineMapper<Article> defaultLineMapper() {
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setDelimiter(DelimitedLineTokenizer.DELIMITER_COMMA);
        delimitedLineTokenizer.setNames("id", "name", "description", "price");

        BeanWrapperFieldSetMapper<Article> beanWrapperFieldSetMapper = new BeanWrapperFieldSetMapper<>();
        beanWrapperFieldSetMapper.setTargetType(Article.class);

        DefaultLineMapper<Article> defaultLineMapper = new DefaultLineMapper<>();
        defaultLineMapper.setLineTokenizer(delimitedLineTokenizer);
        defaultLineMapper.setFieldSetMapper(beanWrapperFieldSetMapper);

        return defaultLineMapper;
    }

    @Bean
    public ItemWriter<Article> articleWriter() {
        return articleRepository::saveAll;
    }

    @Bean
    public ItemProcessor<Article, Article> articleProcessor() {
        return article -> {
            article.setName(article.getName().toUpperCase());
            article.setDescription(article.getDescription().toUpperCase());
            article.setPrice(article.getPrice() * 10);
            return article;
        };
    }

    @Bean
    public JobExecutionListener articleListener() {
        return new ArticleListener();
    }

    @Bean
    public Step articleCSVStep() {
        return new StepBuilder("step", jobRepository)
                .<Article, Article>chunk(BATCH_SIZE, transactionManager)
                .reader(articleCSVReader())
                .processor(articleProcessor())
                .writer(articleWriter())
                .allowStartIfComplete(true) // Allow restart after completion
                .faultTolerant() // Retry on Exception
                .retryLimit(3) // Retry 3 times
                .retry(Exception.class) // Retry on Exception
                .build();
    }

    @Bean
    public Job articleCSVJob() {
        return new JobBuilder("articleJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(articleCSVStep())
                .listener(articleListener())
                .build();
    }
}
