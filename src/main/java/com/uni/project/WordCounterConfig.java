package com.uni.project;

import org.apache.hadoop.conf.Configured;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WordCounterConfig extends Configured {

    @Bean
    public WordMapper wordMapper() {
        return new WordMapper();
    }

    @Bean
    public WordReducer wordReducer() {
        return new WordReducer();
    }
}
