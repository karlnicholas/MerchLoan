package com.github.karlnicholas.merchloan.statement.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class StatementDataSourceAutoconfiguration {
//	@Value("${statementdb.host:localhost}")
//	private String statementdbHost;
//
//	@Value("${maximumPoolSize:100}")
//	private String maximumPoolSize;
//	@Value("${minimumIdle:2}")
//	private String minimumIdle;
//	@Value("${useServerPrepStmts:true}")
//	private String useServerPrepStmts;
//	@Value("${cachePrepStmts:true}")
//	private String cachePrepStmts;
//	@Value("${prepStmtCacheSize:256}")
//	private String prepStmtCacheSize;
//	@Value("${prepStmtCacheSqlLimit:2048}")
//	private String prepStmtCacheSqlLimit;
//
//	@Bean
//	public DataSource getDataSource() {
//		HikariConfig config = new HikariConfig();
//		String databaseUrl = "jdbc:h2:tcp://" + statementdbHost + ":9100/mem:statement;DB_CLOSE_DELAY=-1";
//		config.setJdbcUrl(databaseUrl);
//
//		config.addDataSourceProperty("maximumPoolSize", maximumPoolSize);
//		config.addDataSourceProperty("minimumIdle", minimumIdle);
//		config.addDataSourceProperty("useServerPrepStmts", useServerPrepStmts);
//		config.addDataSourceProperty("cachePrepStmts", cachePrepStmts);
//		config.addDataSourceProperty("prepStmtCacheSize", prepStmtCacheSize);
//		config.addDataSourceProperty("prepStmtCacheSqlLimit", prepStmtCacheSqlLimit);
//		return new HikariDataSource(config);
//	}
}
