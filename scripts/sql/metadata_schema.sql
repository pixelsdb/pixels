-- Create the MySQL metadata database for Pixels.

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema pixels_metadata
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `pixels_metadata` ;
USE `pixels_metadata` ;

-- -----------------------------------------------------
-- Table `pixels_metadata`.`DBS`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`DBS` (
    `DB_ID` BIGINT NOT NULL AUTO_INCREMENT,
    `DB_NAME` VARCHAR(128) NOT NULL,
    `DB_DESC` VARCHAR(4000) NULL,
    PRIMARY KEY (`DB_ID`),
    UNIQUE INDEX `DB_NAME_UNIQUE` (`DB_NAME` ASC))
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`TBLS`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`TBLS` (
    `TBL_ID` BIGINT NOT NULL AUTO_INCREMENT,
    `TBL_NAME` VARCHAR(128) NOT NULL,
    `TBL_TYPE` VARCHAR(128) NULL,
    `TBL_STORAGE_SCHEME` VARCHAR(32) NOT NULL DEFAULT 'file' COMMENT 'The name of the storage scheme of the files stored in all the paths of this table.',
    `TBL_ROW_COUNT` BIGINT NOT NULL DEFAULT 0 COMMENT 'The number of rows in this table.',
    `DBS_DB_ID` BIGINT NOT NULL,
    PRIMARY KEY (`TBL_ID`),
    INDEX `fk_TBLS_DBS_idx` (`DBS_DB_ID` ASC),
    UNIQUE INDEX `TBL_NAME_DB_ID_UNIQUE` (`TBL_NAME` ASC, `DBS_DB_ID` ASC),
    CONSTRAINT `fk_TBLS_DBS`
        FOREIGN KEY (`DBS_DB_ID`)
            REFERENCES `pixels_metadata`.`DBS` (`DB_ID`)
            ON DELETE CASCADE
            ON UPDATE CASCADE)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`COLS`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`COLS` (
    `COL_ID` BIGINT NOT NULL AUTO_INCREMENT,
    `COL_NAME` VARCHAR(128) NOT NULL,
    `COL_TYPE` VARCHAR(128) NOT NULL,
    `COL_CHUNK_SIZE` DOUBLE NOT NULL DEFAULT 0,
    `COL_SIZE` DOUBLE NOT NULL DEFAULT 0,
    `COL_NULL_FRACTION` DOUBLE NOT NULL DEFAULT 0,
    `COL_CARDINALITY` BIGINT NOT NULL DEFAULT 0,
    `COL_RECORD_STATS` BLOB NULL DEFAULT NULL,
    `TBLS_TBL_ID` BIGINT NOT NULL,
    PRIMARY KEY (`COL_ID`),
    INDEX `fk_COLS_TBLS_idx` (`TBLS_TBL_ID` ASC),
    UNIQUE INDEX `COL_NAME_TBL_ID_UNIQUE` (`COL_NAME` ASC, `TBLS_TBL_ID` ASC),
    CONSTRAINT `fk_COLS_TBLS`
        FOREIGN KEY (`TBLS_TBL_ID`)
            REFERENCES `pixels_metadata`.`TBLS` (`TBL_ID`)
            ON DELETE CASCADE
            ON UPDATE CASCADE)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`SCHEMA_VERSIONS`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`SCHEMA_VERSIONS` (
    `SV_ID` BIGINT NOT NULL AUTO_INCREMENT,
    `SV_COLUMNS` MEDIUMTEXT NOT NULL COMMENT 'The json string that contains the ids of the columns owned by this schema version.',
    `SV_TRANS_TS` BIGINT NOT NULL COMMENT 'The transaction timestamp of this schema version.',
    `TBLS_TBL_ID` BIGINT NOT NULL,
    PRIMARY KEY (`SV_ID`),
    INDEX `fk_SCHEMA_VERSIONS_TBLS_idx` (`TBLS_TBL_ID` ASC),
    CONSTRAINT `fk_SCHEMA_VERSIONS_TBLS`
        FOREIGN KEY (`TBLS_TBL_ID`)
            REFERENCES `pixels_metadata`.`TBLS` (`TBL_ID`)
            ON DELETE CASCADE
            ON UPDATE CASCADE)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`LAYOUTS`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`LAYOUTS` (
    `LAYOUT_ID` BIGINT NOT NULL AUTO_INCREMENT,
    `LAYOUT_VERSION` BIGINT NOT NULL COMMENT 'The version of this layout.',
    `LAYOUT_CREATE_AT` BIGINT NOT NULL COMMENT 'The milliseconds of moment since the unix epoch that this layout is created.',
    `LAYOUT_PERMISSION` TINYINT NOT NULL COMMENT '<0 for not readable and writable, 0 for readable only, >0 for readable and writable.',
    `LAYOUT_ORDERED` MEDIUMTEXT NOT NULL COMMENT 'The default order of this layout. It is used to determine the column order in a single-row-group blocks.',
    `LAYOUT_COMPACT` LONGTEXT NOT NULL COMMENT 'the layout strategy, stored as json. It is used to determine how row groups are compacted into a big block.',
    `LAYOUT_SPLITS` LONGTEXT NOT NULL COMMENT 'The suggested split size for access patterns, stored as json.',
    `LAYOUT_PROJECTIONS` LONGTEXT NOT NULL COMMENT 'The projections each maps a set of columns to a different set of paths.',
    `TBLS_TBL_ID` BIGINT NOT NULL,
    `SCHEMA_VERSIONS_SV_ID` BIGINT NOT NULL,
    PRIMARY KEY (`LAYOUT_ID`),
    INDEX `fk_LAYOUTS_TBLS_idx` (`TBLS_TBL_ID` ASC),
    INDEX `fk_LAYOUTS_SCHEMA_VERSIONS_idx` (`SCHEMA_VERSIONS_SV_ID` ASC),
    CONSTRAINT `fk_LAYOUTS_TBLS`
        FOREIGN KEY (`TBLS_TBL_ID`)
            REFERENCES `pixels_metadata`.`TBLS` (`TBL_ID`)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
    CONSTRAINT `fk_LAYOUTS_SCHEMA_VERSIONS`
        FOREIGN KEY (`SCHEMA_VERSIONS_SV_ID`)
            REFERENCES `pixels_metadata`.`SCHEMA_VERSIONS` (`SV_ID`)
            ON DELETE CASCADE
            ON UPDATE CASCADE)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`VIEWS`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`VIEWS` (
    `VIEW_ID` BIGINT NOT NULL AUTO_INCREMENT,
    `VIEW_NAME` VARCHAR(128) NOT NULL,
    `VIEW_TYPE` VARCHAR(128) NULL,
    `VIEW_DATA` LONGTEXT NOT NULL,
    `DBS_DB_ID` BIGINT NOT NULL,
    PRIMARY KEY (`VIEW_ID`),
    INDEX `fk_VIEWS_DBS_idx` (`DBS_DB_ID` ASC),
    CONSTRAINT `fk_VIEWS_DBS`
        FOREIGN KEY (`DBS_DB_ID`)
            REFERENCES `pixels_metadata`.`DBS` (`DB_ID`)
            ON DELETE CASCADE
            ON UPDATE CASCADE)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`RANGE_INDEXES`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`RANGE_INDEXES` (
    `RI_ID` BIGINT NOT NULL AUTO_INCREMENT,
    `RI_KEY_COLUMNS` TEXT NOT NULL COMMENT 'The ids of the key columns, stored in csv format.',
    `TBLS_TBL_ID` BIGINT NOT NULL,
    `SCHEMA_VERSIONS_SV_ID` BIGINT NOT NULL,
    PRIMARY KEY (`RI_ID`),
    INDEX `fk_RANGE_INDEX_TBLS_idx` (`TBLS_TBL_ID` ASC),
    INDEX `fk_RANGE_INDEXES_SCHEMA_VERSIONS_idx` (`SCHEMA_VERSIONS_SV_ID` ASC),
    UNIQUE INDEX `TBL_ID_SV_ID_UNIQUE` (`TBLS_TBL_ID` ASC, `SCHEMA_VERSIONS_SV_ID` ASC) COMMENT 'We ensure every (table, schema_version) has only one range index.',
    CONSTRAINT `fk_RANGE_INDEX_TBLS`
        FOREIGN KEY (`TBLS_TBL_ID`)
            REFERENCES `pixels_metadata`.`TBLS` (`TBL_ID`)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
    CONSTRAINT `fk_RANGE_INDEXES_SCHEMA_VERSIONS`
        FOREIGN KEY (`SCHEMA_VERSIONS_SV_ID`)
            REFERENCES `pixels_metadata`.`SCHEMA_VERSIONS` (`SV_ID`)
            ON DELETE CASCADE
            ON UPDATE CASCADE)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`RANGES`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`RANGES` (
    `RANGE_ID` BIGINT NOT NULL AUTO_INCREMENT,
    `RANGE_MIN` BLOB NOT NULL COMMENT 'The min value of the key column(s).',
    `RANGE_MAX` BLOB NOT NULL COMMENT 'The max value of the key column(s).',
    `RANGE_PARENT_ID` BIGINT NULL,
    `RANGE_INDEXES_RI_ID` BIGINT NOT NULL,
    PRIMARY KEY (`RANGE_ID`),
    INDEX `fk_RANGES_RANGE_INDEXES_idx` (`RANGE_INDEXES_RI_ID` ASC),
    INDEX `fk_RANGES_RANGES_idx` (`RANGE_PARENT_ID` ASC),
    CONSTRAINT `fk_RANGES_RANGE_INDEXES`
        FOREIGN KEY (`RANGE_INDEXES_RI_ID`)
            REFERENCES `pixels_metadata`.`RANGE_INDEXES` (`RI_ID`)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
    CONSTRAINT `fk_RANGES_RANGES`
        FOREIGN KEY (`RANGE_PARENT_ID`)
            REFERENCES `pixels_metadata`.`RANGES` (`RANGE_ID`)
            ON DELETE SET NULL
            ON UPDATE CASCADE)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`PATHS`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`PATHS` (
    `PATH_ID` BIGINT NOT NULL AUTO_INCREMENT,
    `PATH_URI` VARCHAR(512) NOT NULL COMMENT 'The storage path uri containing the storage scheme prefix.',
    `PATH_TYPE` TINYINT NOT NULL COMMENT 'Valid value can be 0 (ordered), 1 (compact), or 2 (projection).',
    `LAYOUTS_LAYOUT_ID` BIGINT NOT NULL,
    `RANGES_RANGE_ID` BIGINT NULL DEFAULT NULL,
    PRIMARY KEY (`PATH_ID`),
    INDEX `fk_PATHS_RANGES_idx` (`RANGES_RANGE_ID` ASC),
    INDEX `fk_PATHS_LAYOUTS_idx` (`LAYOUTS_LAYOUT_ID` ASC),
    UNIQUE INDEX `PATH_URI_UNIQUE` (`PATH_URI` ASC),
    CONSTRAINT `fk_PATHS_RANGES`
        FOREIGN KEY (`RANGES_RANGE_ID`)
            REFERENCES `pixels_metadata`.`RANGES` (`RANGE_ID`)
            ON DELETE SET NULL
            ON UPDATE CASCADE,
    CONSTRAINT `fk_PATHS_LAYOUTS`
        FOREIGN KEY (`LAYOUTS_LAYOUT_ID`)
            REFERENCES `pixels_metadata`.`LAYOUTS` (`LAYOUT_ID`)
            ON DELETE CASCADE
            ON UPDATE CASCADE)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`USERS`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`USERS` (
    `USER_ID` BIGINT NOT NULL AUTO_INCREMENT,
    `USER_NAME` VARCHAR(128) NOT NULL,
    `USER_PASSWORD` VARCHAR(128) NOT NULL,
    `USER_EMAIL` VARCHAR(128) NOT NULL,
    PRIMARY KEY (`USER_ID`),
    UNIQUE INDEX `USER_NAME_UNIQUE` (`USER_NAME` ASC))
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`USER_HAS_DB`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`USER_HAS_DB` (
    `USERS_USER_ID` BIGINT NOT NULL,
    `DBS_DB_ID` BIGINT NOT NULL,
    `USER_DB_PERMITION` TINYINT NOT NULL,
    PRIMARY KEY (`USERS_USER_ID`, `DBS_DB_ID`),
    INDEX `fk_USERS_has_DBS_DBS_idx` (`DBS_DB_ID` ASC),
    INDEX `fk_USERS_has_DBS_USERS_idx` (`USERS_USER_ID` ASC),
    CONSTRAINT `fk_USERS_has_DBS_USERS`
        FOREIGN KEY (`USERS_USER_ID`)
            REFERENCES `pixels_metadata`.`USERS` (`USER_ID`)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
    CONSTRAINT `fk_USERS_has_DBS_DBS`
        FOREIGN KEY (`DBS_DB_ID`)
            REFERENCES `pixels_metadata`.`DBS` (`DB_ID`)
            ON DELETE CASCADE
            ON UPDATE CASCADE)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`PEERS`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`PEERS` (
    `PEER_ID` BIGINT NOT NULL AUTO_INCREMENT,
    `PEER_NAME` VARCHAR(128) NOT NULL COMMENT 'The name of the peer, must be unique.',
    `PEER_LOCATION` VARCHAR(1024) NOT NULL COMMENT 'The geographic location of the peer.',
    `PEER_HOST` VARCHAR(128) NOT NULL COMMENT 'The registered host name or ip address of the peer.',
    `PEER_PORT` INT NOT NULL COMMENT 'The registered port of this peer.',
    `PEER_STORAGE_SCHEME` VARCHAR(32) NOT NULL,
    PRIMARY KEY (`PEER_ID`),
    UNIQUE INDEX `PEER_NAME_UNIQUE` (`PEER_NAME` ASC))
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`PEER_PATHS`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`PEER_PATHS` (
    `PEER_PATH_ID` BIGINT NOT NULL AUTO_INCREMENT,
    `PEER_PATH_URI` VARCHAR(32) NOT NULL,
    `PEER_PATH_COLUMNS` MEDIUMTEXT NOT NULL COMMENT 'The json string that contains the ids of the columns stored in this peer path.',
    `PATHS_PATH_ID` BIGINT NOT NULL,
    `PEERS_PEER_ID` BIGINT NOT NULL,
    PRIMARY KEY (`PEER_PATH_ID`),
    INDEX `fk_PEER_PATHS_PATHS_idx` (`PATHS_PATH_ID` ASC),
    INDEX `fk_PEER_PATHS_PEERS_idx` (`PEERS_PEER_ID` ASC),
    CONSTRAINT `fk_PEER_PATHS_PATHS`
        FOREIGN KEY (`PATHS_PATH_ID`)
            REFERENCES `pixels_metadata`.`PATHS` (`PATH_ID`)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
    CONSTRAINT `fk_PEER_PATHS_PEERS`
        FOREIGN KEY (`PEERS_PEER_ID`)
            REFERENCES `pixels_metadata`.`PEERS` (`PEER_ID`)
            ON DELETE CASCADE
            ON UPDATE CASCADE)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`FILES`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`FILES` (
    `FILE_ID` BIGINT NOT NULL AUTO_INCREMENT,
    `FILE_NAME` VARCHAR(128) NOT NULL,
    `FILE_NUM_RG` INT NOT NULL,
    `FILE_MIN_ROW_ID` BIGINT NOT NULL,
    `FILE_MAX_ROW_ID` BIGINT NOT NULL,
    `PATHS_PATH_ID` BIGINT NOT NULL,
    PRIMARY KEY (`FILE_ID`),
    INDEX `fk_FILES_PATHS_idx` (`PATHS_PATH_ID` ASC),
    UNIQUE INDEX `PATH_ID_FILE_NAME_UNIQUE` (`PATHS_PATH_ID` ASC, `FILE_NAME` ASC),
    INDEX `FILE_ROW_ID_INDEX` USING BTREE (`FILE_MIN_ROW_ID`, `FILE_MAX_ROW_ID`),
    CONSTRAINT `fk_FILES_PATHS`
        FOREIGN KEY (`PATHS_PATH_ID`)
            REFERENCES `pixels_metadata`.`PATHS` (`PATH_ID`)
            ON DELETE CASCADE
            ON UPDATE CASCADE)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`SINGLE_POINT_INDEXES`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`SINGLE_POINT_INDEXES` (
    `SPI_ID` BIGINT NOT NULL,
    `SPI_KEY_COLUMNS` TEXT NOT NULL COMMENT 'The ids of the key columns of this index, stored in json format.',
    `SPI_PRIMARY` TINYINT NOT NULL COMMENT 'True (1) if this single point index is the primary index. There can be only one primary index on a table.',
    `SPI_UNIQUE` TINYINT NOT NULL COMMENT 'True (1) if this single point index is an unique index.',
    `SPI_INDEX_SCHEME` VARCHAR(32) NOT NULL COMMENT 'The index scheme, e.g., rocksdb or rockset, of this single pint index.',
    `TBLS_TBL_ID` BIGINT NOT NULL,
    `SCHEMA_VERSIONS_SV_ID` BIGINT NOT NULL,
    PRIMARY KEY (`SPI_ID`),
    INDEX `fk_SECONDARY_INDEXES_TBLS_idx` (`TBLS_TBL_ID` ASC) VISIBLE,
    INDEX `fk_SECONDARY_INDEXES_SCHEMA_VERSIONS_idx` (`SCHEMA_VERSIONS_SV_ID` ASC) VISIBLE,
    CONSTRAINT `fk_SECONDARY_INDEXES_TBLS`
        FOREIGN KEY (`TBLS_TBL_ID`)
            REFERENCES `pixels_metadata`.`TBLS` (`TBL_ID`)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
    CONSTRAINT `fk_SECONDARY_INDEXES_SCHEMA_VERSIONS`
        FOREIGN KEY (`SCHEMA_VERSIONS_SV_ID`)
            REFERENCES `pixels_metadata`.`SCHEMA_VERSIONS` (`SV_ID`)
            ON DELETE CASCADE
            ON UPDATE CASCADE)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
