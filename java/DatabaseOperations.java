import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseOperations {

    private static final String URL = "jdbc:postgresql://localhost:5432/Aggre";
    private static final String USER = "postgres";
    private static final String PASSWORD = "siddh2003";

    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             Statement statement = connection.createStatement()) {

            connection.setAutoCommit(false);

            createTable(statement);
            createTempTable(statement);
            clearTable(statement);

            connection.commit();

        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    public static void createTable(Statement statement) throws SQLException {
        String createTableQuery = """
            CREATE TABLE IF NOT EXISTS news (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                content TEXT,
                source_name VARCHAR(255),
                published_at TIMESTAMP,
                url VARCHAR(255),
                img VARCHAR(255)
            );
        """;
        statement.execute(createTableQuery);
        System.out.println("Table created or already exists");
    }

    public static void createTempTable(Statement statement) throws SQLException {
        String createTempTableQuery = """
            CREATE TEMPORARY TABLE temp_news (
                title VARCHAR(255),
                content TEXT,
                source_name VARCHAR(255),
                published_at TIMESTAMP,
                url VARCHAR(255),
                img VARCHAR(255)
            );
        """;
        statement.execute(createTempTableQuery);
        System.out.println("Temporary table for news created");
    }

    public static void clearTable(Statement statement) throws SQLException {
        String deleteQuery = "DROP TABLE IF EXISTS news;";
        statement.execute(deleteQuery);
        System.out.println("Existing news table dropped");
    }
}
