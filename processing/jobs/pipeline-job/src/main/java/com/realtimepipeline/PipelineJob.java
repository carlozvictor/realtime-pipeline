package com.realtimepipeline;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Bootstrap de Application Mode: le o SQL versionado em processing/sql/,
 * substitui as variaveis de ambiente e submete tudo como um unico job
 * (fontes + transformacoes + sinks). O pipeline em si continua definido
 * em SQL puro; esta classe existe so para empacotar essa submissao junto
 * com o cluster, dispensando o passo manual de sql-client.
 */
public final class PipelineJob {

    private static final String[] SQL_FILES = {
        "/sql/01_sources.sql",
        "/sql/02_transforms.sql",
        "/sql/03_sinks.sql",
    };

    private PipelineJob() {
    }

    public static void main(String[] args) throws IOException {
        String kafkaTopic = requireEnv("KAFKA_TOPIC");
        String kafkaBootstrap = requireEnv("KAFKA_BOOTSTRAP");
        String clickhouseHost = requireEnv("CLICKHOUSE_HOST");

        List<String> ddlStatements = new ArrayList<>();
        List<String> insertStatements = new ArrayList<>();

        for (String file : SQL_FILES) {
            String sql = readResource(file)
                .replace("${KAFKA_TOPIC}", kafkaTopic)
                .replace("${KAFKA_BOOTSTRAP}", kafkaBootstrap)
                .replace("${CLICKHOUSE_HOST}", clickhouseHost);

            for (String statement : sql.split(";")) {
                String trimmed = statement.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                if (trimmed.toUpperCase(Locale.ROOT).startsWith("INSERT")) {
                    insertStatements.add(trimmed);
                } else {
                    ddlStatements.add(trimmed);
                }
            }
        }

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        for (String ddl : ddlStatements) {
            tEnv.executeSql(ddl);
        }

        StatementSet statementSet = tEnv.createStatementSet();
        for (String insert : insertStatements) {
            statementSet.addInsertSql(insert);
        }
        statementSet.execute();
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException("Variavel de ambiente obrigatoria ausente: " + name);
        }
        return value;
    }

    private static String readResource(String path) throws IOException {
        try (InputStream in = PipelineJob.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IOException("Recurso nao encontrado no classpath: " + path);
            }
            StringBuilder sb = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line).append('\n');
                }
            }
            return sb.toString();
        }
    }
}
