package com.github.karlnicholas.merchloan.sqlutil;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
@ConditionalOnClass(DataSource.class)
public class DataSourceAutoconfiguration {
	@Value("${database-url:jdbc:h2:mem:test;DB_CLOSE_DELAY=-1}")
    private String databaseUrl;

	@Value("${maximumPoolSize:100}")
	private String maximumPoolSize;
	@Value("${minimumIdle:2}")
	private String minimumIdle;
	@Value("${useServerPrepStmts:true}")
	private String useServerPrepStmts;
	@Value("${cachePrepStmts:true}")
	private String cachePrepStmts;
	@Value("${prepStmtCacheSize:256}")
	private String prepStmtCacheSize;
	@Value("${prepStmtCacheSqlLimit:2048}")
	private String prepStmtCacheSqlLimit;

	@Bean
	@ConditionalOnMissingBean
	public DataSource getDataSource()  {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(databaseUrl);

		config.addDataSourceProperty("maximumPoolSize", maximumPoolSize);
		config.addDataSourceProperty("minimumIdle", minimumIdle);
		config.addDataSourceProperty("useServerPrepStmts", useServerPrepStmts);
		config.addDataSourceProperty("cachePrepStmts", cachePrepStmts);
		config.addDataSourceProperty("prepStmtCacheSize", prepStmtCacheSize);
		config.addDataSourceProperty("prepStmtCacheSqlLimit", prepStmtCacheSqlLimit);
		return new HikariDataSource(config);
	}
}
