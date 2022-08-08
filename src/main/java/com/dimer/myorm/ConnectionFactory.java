package com.dimer.myorm;

import java.sql.Connection;

public abstract class ConnectionFactory {
    private static Connection connection;
    private static ConnectionFactory factory;

    public static void setFactory(ConnectionFactory connectionFactory) {
        factory = connectionFactory;
    }

    protected static Connection getConnection() {
        if (factory == null) {
            throw new RuntimeException("Connection factory not initialized!");
        }

        if (connection == null) {
            connection = factory.createConnection();
        }

        return connection;
    }

    protected abstract Connection createConnection();
}
