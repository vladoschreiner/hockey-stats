package biz.schr.cdcdemo.loader;

import biz.schr.cdcdemo.Config;
import biz.schr.cdcdemo.dto.Player;
import com.hazelcast.map.MapLoader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PlayerMapLoader
        implements MapLoader<Long, Player> {

    private final Connection con;


    public PlayerMapLoader() {
        try{
            con = DriverManager.getConnection(Config.dbURL, Config.dbUsername, Config.dbPassword);
        } catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public Player load(Long id) {
        ResultSet rs = null;

        try {
            rs = con.createStatement().executeQuery("select hracid, jmeno, prijmeni from hrac where hracid = " + id);
            while (rs.next()) {
                return new Player(rs.getLong(1), rs.getString(2), rs.getString(3));
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
    public Map<Long, Player> loadAll(Collection<Long> collection) {

        Map<Long, Player> m = new HashMap<Long, Player>();

        ResultSet rs = null;
        String ids = collection.stream().map(String::valueOf).collect(Collectors.joining(", "));

        try {
            rs = con.createStatement().executeQuery(
                    "SELECT hracid, jmeno, prijmeni FROM hrac WHERE hracid in ("+ ids + ")");

            while (rs.next()) {
                m.put(rs.getLong(1), new Player(rs.getLong(1), rs.getString(2), rs.getString(3)));
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

            rs = con.createStatement().executeQuery("select hracid from hrac");
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