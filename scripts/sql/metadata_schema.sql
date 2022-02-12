-- MySQL dump 10.13  Distrib 8.0.27, for Linux (x86_64)
--
-- Database: pixels_metadata
-- ------------------------------------------------------
-- Server version	8.0.27-0ubuntu0.20.04.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `COLS`
--

DROP TABLE IF EXISTS `COLS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `COLS` (
  `COL_ID` int NOT NULL AUTO_INCREMENT,
  `COL_NAME` varchar(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `COL_TYPE` varchar(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `COL_SIZE` double NOT NULL,
  `TBLS_TBL_ID` int NOT NULL,
  PRIMARY KEY (`COL_ID`),
  UNIQUE KEY `COL_NAME_TBL_ID_UNIQUE` (`COL_NAME`,`TBLS_TBL_ID`),
  KEY `fk_COLS_TBLS1_idx` (`TBLS_TBL_ID`),
  CONSTRAINT `fk_COLS_TBLS1` FOREIGN KEY (`TBLS_TBL_ID`) REFERENCES `TBLS` (`TBL_ID`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `DBS`
--

DROP TABLE IF EXISTS `DBS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `DBS` (
  `DB_ID` int NOT NULL AUTO_INCREMENT,
  `DB_NAME` varchar(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `DB_DESC` varchar(4000) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`DB_ID`),
  UNIQUE KEY `DB_NAME_UNIQUE` (`DB_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `LAYOUTS`
--

DROP TABLE IF EXISTS `LAYOUTS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `LAYOUTS` (
  `LAYOUT_ID` int NOT NULL AUTO_INCREMENT,
  `LAYOUT_VERSION` int NOT NULL COMMENT 'The version of this layout.',
  `LAYOUT_CREATE_AT` bigint NOT NULL COMMENT 'The time (output of System.currentTimeMillis()) on which this layout is created.',
  `LAYOUT_PERMISSION` tinyint NOT NULL COMMENT '<0 for not readable and writable, 0 for readable only, >0 for readable and writable.',
  `LAYOUT_ORDER` mediumtext CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT 'The default order of this layout. It is used to determine the column order in a single-row-group blocks.',
  `LAYOUT_ORDER_PATH` varchar(4000) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT 'The directory where single-row-group files are initially stored.',
  `LAYOUT_COMPACT` longtext CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT 'The layout strategy, stored as json. It is used to determine how row groups are compacted into a big block.',
  `LAYOUT_COMPACT_PATH` varchar(4000) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT 'The path where compacted big files are stored.',
  `LAYOUT_SPLITS` longtext CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT 'The suggested split size for access patterns, stored as json.',
  `LAYOUT_PROJECTIONS` longtext COLLATE utf8_bin NOT NULL,
  `TBLS_TBL_ID` int NOT NULL,
  PRIMARY KEY (`LAYOUT_ID`),
  KEY `fk_LAYOUT_TBLS1_idx` (`TBLS_TBL_ID`),
  CONSTRAINT `fk_LAYOUT_TBLS1` FOREIGN KEY (`TBLS_TBL_ID`) REFERENCES `TBLS` (`TBL_ID`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `TBLS`
--

DROP TABLE IF EXISTS `TBLS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `TBLS` (
  `TBL_ID` int NOT NULL AUTO_INCREMENT,
  `TBL_NAME` varchar(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `TBL_TYPE` varchar(128) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  `TBL_STORAGE_SCHEME` varchar(32) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT 'hdfs',
  `DBS_DB_ID` int NOT NULL,
  PRIMARY KEY (`TBL_ID`),
  UNIQUE KEY `TBL_NAME_DB_ID_UNIQUE` (`TBL_NAME`,`DBS_DB_ID`),
  KEY `fk_TBLS_DBS_idx` (`DBS_DB_ID`),
  CONSTRAINT `fk_TBLS_DBS` FOREIGN KEY (`DBS_DB_ID`) REFERENCES `DBS` (`DB_ID`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2022-02-11 13:27:02
