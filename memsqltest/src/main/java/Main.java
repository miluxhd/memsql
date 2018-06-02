import java.sql.*;
import java.util.Properties;
import java.util.concurrent.*;
public class Main {
    static long th_query_count = 1000000;
    static int thread_count = 24;
    private static void executeSQL(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
    private static void ResetEnvironment() throws SQLException {
        Properties p = new Properties();
        p.put("user", "root");
        p.put("password", "");
        try (Connection conn = DriverManager.getConnection("jdbc:mysql://memsql-1:3306/", p)) {
            for (String query: new String[] {
                    "DROP DATABASE IF EXISTS test",
                    "CREATE DATABASE test",
                    "USE test",
                    "CREATE TABLE mem_table (name VARCHAR(32),id INT AUTO_INCREMENT PRIMARY KEY)"
            }) {
                executeSQL(conn, query);
            }
        }
    }
    private static void worker() {
        Properties properties = new Properties();
        properties.put("user", "root");
        properties.put("password", "");
        try (Connection conn = DriverManager.getConnection("jdbc:mysql://memsql-1:3306/",properties)) {
            executeSQL(conn, "USE test");
            long start = System.currentTimeMillis();
            int i=0;
            for (i=0; i < th_query_count ; i++)
                executeSQL(conn, "INSERT INTO mem_table(name,id) VALUES ('milux',NULL)");
            System.out.println(Thread.currentThread()+",total time : "+(System.currentTimeMillis()-start)+",total queries : "+i);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {
        long start = System.currentTimeMillis() ;
        Class.forName("com.mysql.jdbc.Driver");
        ResetEnvironment();
        ExecutorService executor = Executors.newFixedThreadPool(thread_count);
        for (int i = 0; i < thread_count; i++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    worker();
                }
            });
        }
        executor.shutdown();
        System.out.println("done");
        while (true)
        if (executor.isTerminated()) {
            System.out.println("insert speed : "+(th_query_count*thread_count*1000)/(System.currentTimeMillis() - start)+" QPS");
            System.exit(0);
        }
    }
}


