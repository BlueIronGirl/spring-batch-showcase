package de.alice.springbatch.service;

import de.alice.springbatch.entity.Article;
import de.alice.springbatch.repository.ArticleRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ArticleService {
    private final ArticleRepository articleRepository;

    @Autowired
    public ArticleService(ArticleRepository articleRepository) {
        this.articleRepository = articleRepository;
    }

    public List<Article> selectAllArticles() {
        return articleRepository.findAll();
    }
}
