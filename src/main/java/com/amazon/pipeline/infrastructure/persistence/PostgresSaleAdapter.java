package com.amazon.pipeline.infrastructure.persistence;

import com.amazon.pipeline.domain.AmazonSale;
import com.amazon.pipeline.domain.SaleRepository;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.PCollection;

public class PostgresSaleAdapter implements SaleRepository {
    @Override
    public void save(PCollection<AmazonSale> sales) {
        sales.apply("WriteToDB", JdbcIO.<AmazonSale>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                "org.postgresql.Driver", "jdbc:postgresql://localhost:5432/amazon_sales")
                        .withUsername("postgres").withPassword("secret_pass"))
                .withStatement("INSERT INTO sales (id, amount, category) " +
                        "VALUES (?, ?, ?) " +
                        "ON CONFLICT (id) DO UPDATE SET " +
                        "amount = EXCLUDED.amount, " +
                        "category = EXCLUDED.category")
                .withPreparedStatementSetter((sale, statement) -> {
                    statement.setString(1, sale.id());
                    statement.setDouble(2, sale.amount());
                    statement.setString(3, sale.category());
                }));
    }
}