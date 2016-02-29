package org.apache.phoenix.jdbc;

import java.sql.SQLException;

public class TxPhoenixUtils {

    public static PhoenixEmbeddedDriver.ConnectionInfo createConnectionInfo(String phoenixJdbcUrl) throws SQLException {
        return PhoenixEmbeddedDriver.ConnectionInfo.create(phoenixJdbcUrl);
    }
}
