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
import io.pixelsdb.pixels.daemon.metadata.dao.PeerPathDao;
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
public class RdbPeerPathDao extends PeerPathDao
{
    public RdbPeerPathDao() {}

    private static final Logger log = LogManager.getLogger(RdbPeerPathDao.class);

    private static final MetaDBUtil db = MetaDBUtil.Instance();

    @Override
    public MetadataProto.PeerPath getById(long id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM PEER_PATHS WHERE PEER_PATH_ID=" + id);
            if (rs.next())
            {
                MetadataProto.PeerPath peerPath = MetadataProto.PeerPath.newBuilder()
                        .setId(id)
                        .setUri(rs.getString("PEER_PATH_URI"))
                        .setColumns(rs.getString("PEER_PATH_COLUMNS"))
                        .setPathId(rs.getLong("PATHS_PATH_ID"))
                        .setPeerId(rs.getLong("PEERS_PEER_ID")).build();
                return peerPath;
            }
        } catch (SQLException e)
        {
            log.error("getById in RdbPeerPathDao", e);
        }

        return null;
    }

    @Override
    public List<MetadataProto.PeerPath> getAllByPath(MetadataProto.Path path)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM PEER_PATHS WHERE PATHS_PATH_ID=" + path.getId());
            List<MetadataProto.PeerPath> peerPaths = new ArrayList<>();
            while (rs.next())
            {
                MetadataProto.PeerPath peerPath = MetadataProto.PeerPath.newBuilder()
                        .setId(rs.getLong("PEER_PATH_ID"))
                        .setUri(rs.getString("PEER_PATH_URI"))
                        .setColumns(rs.getString("PEER_PATH_COLUMNS"))
                        .setPathId(path.getId())
                        .setPeerId(rs.getLong("PEERS_PEER_ID")).build();
                peerPaths.add(peerPath);
            }
            return peerPaths;
        } catch (SQLException e)
        {
            log.error("getAllByPath in RdbPeerPathDao", e);
        }

        return null;
    }

    @Override
    public List<MetadataProto.PeerPath> getAllByPeer(MetadataProto.Peer peer)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM PEER_PATHS WHERE PEERS_PEER_ID=" + peer.getId());
            List<MetadataProto.PeerPath> peerPaths = new ArrayList<>();
            while (rs.next())
            {
                MetadataProto.PeerPath peerPath = MetadataProto.PeerPath.newBuilder()
                        .setId(rs.getLong("PEER_PATH_ID"))
                        .setUri(rs.getString("PEER_PATH_URI"))
                        .setColumns(rs.getString("PEER_PATH_COLUMNS"))
                        .setPathId(rs.getLong("PATHS_PATH_ID"))
                        .setPeerId(peer.getId()).build();
                peerPaths.add(peerPath);
            }
            return peerPaths;
        } catch (SQLException e)
        {
            log.error("getAllByPeer in RdbPeerPathDao", e);
        }

        return null;
    }

    @Override
    public boolean exists(MetadataProto.PeerPath peerPath)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            String sql = "SELECT 1 FROM PATHS WHERE PEER_PATH_ID=" + peerPath.getId();
            ResultSet rs = st.executeQuery(sql);
            if (rs.next())
            {
                return true;
            }
        } catch (SQLException e)
        {
            log.error("exists in RdbPeerPathDao", e);
        }

        return false;
    }

    @Override
    public boolean insert(MetadataProto.PeerPath peerPath)
    {
        Connection conn = db.getConnection();
        String sql = "INSERT INTO PEER_PATHS(" +
                "`PEER_PATH_URI`," +
                "`PEER_PATH_COLUMNS`," +
                "`PATHS_PATH_ID`," +
                "`PEERS_PEER_ID`) VALUES (?,?,?,?)";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, peerPath.getUri());
            pst.setString(2, peerPath.getColumns());
            pst.setLong(3, peerPath.getPathId());
            pst.setLong(4, peerPath.getPeerId());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("insert in RdbPeerPathDao", e);
        }

        return false;
    }

    @Override
    public boolean update(MetadataProto.PeerPath peerPath)
    {
        Connection conn = db.getConnection();
        String sql = "UPDATE PEER_PATHS\n" +
                "SET\n" +
                "`PEER_PATH_URI` = ?," +
                "`PEER_PATH_COLUMNS` = ?\n" +
                "WHERE `PEER_PATH_ID` = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, peerPath.getUri());
            pst.setString(2, peerPath.getColumns());
            pst.setLong(3, peerPath.getId());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("update in RdbPeerPathDao", e);
        }

        return false;
    }

    @Override
    public boolean deleteById(long id)
    {
        Connection conn = db.getConnection();
        String sql = "DELETE FROM PEER_PATHS WHERE PEER_PATH_ID=?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setLong(1, id);
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("deleteById in RdbPeerPathDao", e);
        }

        return false;
    }
}
