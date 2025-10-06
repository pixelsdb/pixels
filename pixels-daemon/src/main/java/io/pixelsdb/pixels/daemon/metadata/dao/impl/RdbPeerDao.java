/*
 * Copyright 2023 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.daemon.metadata.dao.impl;

import io.pixelsdb.pixels.common.utils.MetaDBUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.PeerDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hank
 * @create 2023-06-10
 */
public class RdbPeerDao extends PeerDao
{
    private static final Logger log = LogManager.getLogger(RdbPeerDao.class);

    public RdbPeerDao() {}

    private static final MetaDBUtil db = MetaDBUtil.Instance();

    @Override
    public MetadataProto.Peer getById(long id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM PEERS WHERE PEER_ID=" + id);
            if (rs.next())
            {
                MetadataProto.Peer peer = MetadataProto.Peer.newBuilder()
                        .setId(id)
                        .setName(rs.getString("PEER_NAME"))
                        .setLocation(rs.getString("PEER_LOCATION"))
                        .setHost(rs.getString("PEER_HOST"))
                        .setPort(rs.getInt("PEER_PORT"))
                        .setStorageScheme(rs.getString("PEER_STORAGE_SCHEME")).build();
                return peer;
            }
        } catch (SQLException e)
        {
            log.error("getById in RdbPeerDao", e);
        }

        return null;
    }

    @Override
    public List<MetadataProto.Peer> getAll()
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM PEERS");
            List<MetadataProto.Peer> peers = new ArrayList<>();
            if (rs.next())
            {
                MetadataProto.Peer peer = MetadataProto.Peer.newBuilder()
                        .setId(rs.getLong("PEER_ID"))
                        .setName(rs.getString("PEER_NAME"))
                        .setLocation(rs.getString("PEER_LOCATION"))
                        .setHost(rs.getString("PEER_HOST"))
                        .setPort(rs.getInt("PEER_PORT"))
                        .setStorageScheme(rs.getString("PEER_STORAGE_SCHEME")).build();
                peers.add(peer);
            }
            return peers;
        } catch (SQLException e)
        {
            log.error("getAll in RdbPeerDao", e);
        }

        return null;
    }

    @Override
    public MetadataProto.Peer getByName(String name)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM PEERS WHERE PEER_NAME='" + name + "'");
            if (rs.next())
            {
                MetadataProto.Peer peer = MetadataProto.Peer.newBuilder()
                        .setId(rs.getLong("PEER_ID"))
                        .setName(name)
                        .setLocation(rs.getString("PEER_LOCATION"))
                        .setHost(rs.getString("PEER_HOST"))
                        .setPort(rs.getInt("PEER_PORT"))
                        .setStorageScheme(rs.getString("PEER_STORAGE_SCHEME")).build();
                return peer;
            }
        } catch (SQLException e)
        {
            log.error("getByName in RdbPeerDao", e);
        }

        return null;
    }

    @Override
    public boolean exists(MetadataProto.Peer peer)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            String sql = "SELECT 1 FROM PEERS WHERE PEER_ID=" + peer.getId() +
                    " OR PEER_NAME='" + peer.getName() + "'";
            ResultSet rs = st.executeQuery(sql);
            if (rs.next())
            {
                return true;
            }
        } catch (SQLException e)
        {
            log.error("exists in RdbPeerDao", e);
        }

        return false;
    }

    @Override
    public boolean insert(MetadataProto.Peer peer)
    {
        Connection conn = db.getConnection();
        String sql = "INSERT INTO PEERS(" +
                "`PEER_NAME`," +
                "`PEER_LOCATION`," +
                "`PEER_HOST`," +
                "`PEER_PORT`," +
                "`PEER_STORAGE_SCHEME`) VALUES (?,?,?,?,?)";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, peer.getName());
            pst.setString(2, peer.getLocation());
            pst.setString(3, peer.getHost());
            pst.setInt(4, peer.getPort());
            pst.setString(5, peer.getStorageScheme());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("insert in RdbPeerDao", e);
        }

        return false;
    }

    @Override
    public boolean update(MetadataProto.Peer peer)
    {
        Connection conn = db.getConnection();
        String sql = "UPDATE PEERS\n" +
                "SET\n" +
                "`PEER_LOCATION` = ?," +
                "`PEER_HOST` = ?," +
                "`PEER_PORT` = ?," +
                "`PEER_STORAGE_SCHEME` = ?\n" +
                "WHERE `PEER_ID` = ? OR `PEER_NAME` = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, peer.getLocation());
            pst.setString(2, peer.getHost());
            pst.setInt(3, peer.getPort());
            pst.setString(4, peer.getStorageScheme());
            pst.setLong(5, peer.getId());
            pst.setString(6, peer.getName());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("update in RdbPeerDao", e);
        }

        return false;
    }

    @Override
    public boolean deleteById(long id)
    {
        Connection conn = db.getConnection();
        String sql = "DELETE FROM PEERS WHERE PEER_ID=?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setLong(1, id);
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("deleteById in RdbPeerDao", e);
        }

        return false;
    }

    @Override
    public boolean deleteByName(String name)
    {
        Connection conn = db.getConnection();
        String sql = "DELETE FROM PEERS WHERE PEER_NAME=?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, name);
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("deleteByName in RdbPeerDao", e);
        }

        return false;
    }
}
