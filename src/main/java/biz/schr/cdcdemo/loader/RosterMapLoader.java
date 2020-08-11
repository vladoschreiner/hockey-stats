package biz.schr.cdcdemo.loader;

import biz.schr.cdcdemo.Config;
import com.hazelcast.map.MapLoader;
import java.sql.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RosterMapLoader implements MapLoader<Long, Long> {

    private final Connection con;

    public RosterMapLoader() {
        try{
            con = DriverManager.getConnection(Config.dbURL, Config.dbUsername,
                    Config.dbPassword);
        } catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public Long load(Long id) {
        ResultSet rs = null;

        try {
            rs = con.createStatement().executeQuery("select hracid from soupiska where soupiskaid = " + id);
            while (rs.next()) {
                return rs.getLong(1);
            }

            return null;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return null;
    }

    @Override
    public Map<Long,Long> loadAll(Collection<Long> collection) {

        Map<Long, Long> m = new HashMap<Long,Long>();

        ResultSet rs = null;
        String ids = collection.stream().map(String::valueOf).collect(Collectors.joining(", "));

        try {
            rs = con.createStatement().executeQuery(
                    "SELECT soupiskaid, hracid FROM soupiska WHERE soupiskaid in ("+ ids + ")");

            while (rs.next()) {
                m.put(rs.getLong(1), rs.getLong(2));
            }

            return m;

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return m;
    }

    @Override
    public Iterable loadAllKeys() {

        ResultSet rs = null;
        List<Long> l = new LinkedList<Long>();

        try {

            rs = con.createStatement().executeQuery("select soupiskaid from soupiska");
            while (rs.next()) {
                l.add(rs.getLong(1));
            }

            return l;

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return l;
    }
}
