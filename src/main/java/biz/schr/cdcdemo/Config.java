package biz.schr.cdcdemo;

public class Config {

    // DB Connection config
    public static final String dbAddress = "192.168.56.101";
    public static final int dbPort = 3306;
    public static final String dbUsername = "dbz";
    public static final String dbPassword = "dbz";
    public static final String dbClusterName = "dbserver1";
    public static final String dbDbWhitelist = "ahlcz5";
    public static final String[] dbTableWhitelist = {"ahlcz5.hrac", "ahlcz5.soupiska", "ahlcz5.branka"};
}
