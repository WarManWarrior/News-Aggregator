import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class JDBCExample {

    static final String DB_URL = "jdbc:postgresql://localhost:5432/Aggre";
    static final String USER = "postgres";
    static final String PASS = "siddh2003";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;

        try {
            // Load the PostgreSQL JDBC driver
            Class.forName("org.postgresql.Driver");
            System.out.println("PostgreSQL JDBC Driver Registered!");

            // Open a connection
            System.out.println("Connecting to the database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            // Execute SQL statements
            stmt = conn.createStatement();
            String delete1 = "DROP TABLE IF EXISTS news;";
            String delete2 = "DROP TABLE IF EXISTS sample;";
            String delete3 = "DROP TABLE IF EXISTS news_article;";
            stmt.executeUpdate(delete1);
            stmt.executeUpdate(delete2);
            stmt.executeUpdate(delete3);
            System.out.println("Table deleted successfully!");
            String create1 = "CREATE TABLE IF NOT EXISTS news " +
                    "(id SERIAL PRIMARY KEY, " +
                    "title VARCHAR(255), " +
                    "content TEXT, " +
                    "source_name VARCHAR(255), " +
                    "published_at TIMESTAMP, " +
                    "url VARCHAR(255), " +
                    "img VARCHAR(255));";
            String create2 = "CREATE TABLE sample" +
                    "(id SERIAL PRIMARY KEY," +
                    "title TEXT NOT NULL," +
                    "content TEXT NOT NULL," +
                    "original_url TEXT NOT NULL," +
                    "image_url TEXT);";
            String create3 = "CREATE TABLE news_article" +
                    "(id SERIAL PRIMARY KEY," +
                    "title VARCHAR(255)," +
                    "content TEXT," +
                    "original_url TEXT," +
                    "image_url TEXT," +
                    "score VARCHAR(255));";
            stmt.executeUpdate(create1);
            stmt.executeUpdate(create2);
            stmt.executeUpdate(create3);

            System.out.println("Table created successfully!");

            // Clean-up environment
            stmt.close();
            conn.close();

        } catch (Exception e) {
            System.out.println("An error occurred: " + e.getMessage());
        }
    }
}
