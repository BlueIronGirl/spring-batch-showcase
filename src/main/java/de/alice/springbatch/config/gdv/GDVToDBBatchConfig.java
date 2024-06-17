package de.alice.springbatch.config.gdv;

import de.alice.springbatch.dto.GDVSatzart0001;
import de.alice.springbatch.dto.GDVSatzart0052;
import de.alice.springbatch.repository.ArticleRepository;
import de.alice.springbatch.util.BlankLineRecordSeparatorPolicy;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.mapping.PatternMatchingCompositeLineMapper;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class GDVToDBBatchConfig {
    private final ArticleRepository articleRepository;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private static final int BATCH_SIZE = 5;

    @Autowired
    public GDVToDBBatchConfig(ArticleRepository articleRepository, JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        this.articleRepository = articleRepository;
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
    }

    @Bean
    public ItemReader<Object> gdvReader() {
        FlatFileItemReader<Object> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("gdv.txt"));
        reader.setLineMapper(lineMapper());
        reader.setRecordSeparatorPolicy(new BlankLineRecordSeparatorPolicy());
        return reader;
    }

    private PatternMatchingCompositeLineMapper<Object> lineMapper() {
        PatternMatchingCompositeLineMapper<Object> patternMatchingMapper = new PatternMatchingCompositeLineMapper<>();

        patternMatchingMapper.setTokenizers(getTokenizers());
        patternMatchingMapper.setFieldSetMappers(getMappers());

        return patternMatchingMapper;
    }

    private Map<String, LineTokenizer> getTokenizers() {
        Map<String, LineTokenizer> tokenizers = new HashMap<>();
        tokenizers.put("0001*", satzart0001Tokenizer());
        tokenizers.put("0052*", satzart0052Tokenizer());
        return tokenizers;
    }

    @Bean
    public FixedLengthTokenizer satzart0001Tokenizer() {
        FixedLengthTokenizer fixedLengthTokenizer = new FixedLengthTokenizer();
        fixedLengthTokenizer.setNames("satzart", "vuNummer", "absender", "adressat");
        fixedLengthTokenizer.setColumns(new Range(1, 4), new Range(5, 9), new Range(10, 11), new Range(12, 13));
        fixedLengthTokenizer.setStrict(false);
        return fixedLengthTokenizer;
    }

    @Bean
    public FixedLengthTokenizer satzart0052Tokenizer() {
        FixedLengthTokenizer fixedLengthTokenizer = new FixedLengthTokenizer();
        fixedLengthTokenizer.setNames("satzart", "vuNummer", "buendelungskennzeichen", "sparte");
        fixedLengthTokenizer.setColumns(new Range(1, 4), new Range(5, 9), new Range(10, 10), new Range(11, 13));
        fixedLengthTokenizer.setStrict(false);
        return fixedLengthTokenizer;
    }

    private Map<String, FieldSetMapper<Object>> getMappers() {
        BeanWrapperFieldSetMapper<Object> mapperSatzart0001 = new BeanWrapperFieldSetMapper<>();
        mapperSatzart0001.setTargetType(GDVSatzart0001.class);

        BeanWrapperFieldSetMapper<Object> mapperSatzart0052 = new BeanWrapperFieldSetMapper<>();
        mapperSatzart0052.setTargetType(GDVSatzart0052.class);

        Map<String, FieldSetMapper<Object>> mappers = new HashMap<>();
        mappers.put("0001*", mapperSatzart0001);
        mappers.put("0052*", mapperSatzart0052);
        return mappers;
    }


    @Bean
    public ItemWriter<Object> gdvWriter() {
        return System.out::println;
    }

    @Bean
    public Step gdvStep() {
        return new StepBuilder("gdvStep", jobRepository)
                .chunk(BATCH_SIZE, transactionManager)
                .reader(gdvReader())
                .writer(gdvWriter())
                .allowStartIfComplete(true) // Allow restart after completion
                .faultTolerant() // Retry on Exception
                .retryLimit(3) // Retry 3 times
                .retry(Exception.class) // Retry on Exception
                .build();
    }

    @Bean
    public Job gdvJob() {
        return new JobBuilder("gdvJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(gdvStep())
                .build();
    }

}
