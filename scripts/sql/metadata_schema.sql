-- Create the MySQL metadata database for Pixels.

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema pixels_metadata
-- -----------------------------------------------------

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
                                                       UNIQUE INDEX `DB_NAME_UNIQUE` (`DB_NAME` ASC) VISIBLE)
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
                                                        `TBL_STORAGE_SCHEME` VARCHAR(32) NOT NULL DEFAULT 'file',
                                                        `TBL_ROW_COUNT` BIGINT NOT NULL DEFAULT 0,
                                                        `DBS_DB_ID` BIGINT NOT NULL,
                                                        PRIMARY KEY (`TBL_ID`),
                                                        INDEX `fk_TBLS_DBS_idx` (`DBS_DB_ID` ASC) VISIBLE,
                                                        UNIQUE INDEX `TBL_NAME_DB_ID_UNIQUE` (`TBL_NAME` ASC, `DBS_DB_ID` ASC) VISIBLE,
                                                        CONSTRAINT `fk_TBLS_DBS`
                                                            FOREIGN KEY (`DBS_DB_ID`)
                                                                REFERENCES `pixels_metadata`.`DBS` (`DB_ID`)
                                                                ON DELETE RESTRICT
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
                                                        INDEX `fk_COLS_TBLS_idx` (`TBLS_TBL_ID` ASC) VISIBLE,
                                                        UNIQUE INDEX `COL_NAME_TBL_ID_UNIQUE` (`COL_NAME` ASC, `TBLS_TBL_ID` ASC) VISIBLE,
                                                        CONSTRAINT `fk_COLS_TBLS`
                                                            FOREIGN KEY (`TBLS_TBL_ID`)
                                                                REFERENCES `pixels_metadata`.`TBLS` (`TBL_ID`)
                                                                ON DELETE RESTRICT
                                                                ON UPDATE CASCADE)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`SVS`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`SVS` (
                                                       `SV_ID` INT NOT NULL AUTO_INCREMENT,
                                                       `SV_COLUMNS` MEDIUMTEXT NOT NULL,
                                                       `SV_TIMESTAMP` BIGINT NOT NULL,
                                                       `TBLS_TBL_ID` BIGINT NOT NULL,
                                                       PRIMARY KEY (`SV_ID`),
                                                       INDEX `fk_SCHEMA_VERSIONS_TBLS_idx` (`TBLS_TBL_ID` ASC) VISIBLE,
                                                       CONSTRAINT `fk_SCHEMA_VERSIONS_TBLS`
                                                           FOREIGN KEY (`TBLS_TBL_ID`)
                                                               REFERENCES `pixels_metadata`.`TBLS` (`TBL_ID`)
                                                               ON DELETE RESTRICT
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
                                                           `LAYOUT_CREATE_AT` BIGINT NOT NULL COMMENT 'The time (output of System.currentTimeMillis()) on which this layout is created.',
                                                           `LAYOUT_PERMISSION` TINYINT NOT NULL COMMENT '<0 for not readable and writable, 0 for readable only, >0 for readable and writable.',
                                                           `LAYOUT_ORDERED` MEDIUMTEXT NOT NULL COMMENT 'The default order of this layout. It is used to determine the column order in a single-row-group blocks.',
                                                           `LAYOUT_COMPACT` LONGTEXT NOT NULL COMMENT 'the layout strategy, stored as json. It is used to determine how row groups are compacted into a big block.',
                                                           `LAYOUT_SPLITS` LONGTEXT NOT NULL COMMENT 'The suggested split size for access patterns, stored as json.',
                                                           `LAYOUT_PROJECTIONS` LONGTEXT NOT NULL,
                                                           `SVS_SV_ID` INT NOT NULL,
                                                           `TBLS_TBL_ID` BIGINT NOT NULL,
                                                           PRIMARY KEY (`LAYOUT_ID`),
                                                           INDEX `fk_LAYOUTS_SVS_idx` (`SVS_SV_ID` ASC) VISIBLE,
                                                           INDEX `fk_LAYOUTS_TBLS_idx` (`TBLS_TBL_ID` ASC) VISIBLE,
                                                           CONSTRAINT `fk_LAYOUTS_SVS`
                                                               FOREIGN KEY (`SVS_SV_ID`)
                                                                   REFERENCES `pixels_metadata`.`SVS` (`SV_ID`)
                                                                   ON DELETE RESTRICT
                                                                   ON UPDATE CASCADE,
                                                           CONSTRAINT `fk_LAYOUTS_TBLS`
                                                               FOREIGN KEY (`TBLS_TBL_ID`)
                                                                   REFERENCES `pixels_metadata`.`TBLS` (`TBL_ID`)
                                                                   ON DELETE RESTRICT
                                                                   ON UPDATE CASCADE)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`VIEWS`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`VIEWS` (
                                                         `VIEW_ID` BIGINT NOT NULL,
                                                         `VIEW_NAME` VARCHAR(128) NOT NULL,
                                                         `VIEW_TYPE` VARCHAR(128) NULL,
                                                         `VIEW_DATA` LONGTEXT NOT NULL,
                                                         `DBS_DB_ID` BIGINT NOT NULL,
                                                         PRIMARY KEY (`VIEW_ID`),
                                                         INDEX `fk_VIEWS_DBS_idx` (`DBS_DB_ID` ASC) VISIBLE,
                                                         CONSTRAINT `fk_VIEWS_DBS`
                                                             FOREIGN KEY (`DBS_DB_ID`)
                                                                 REFERENCES `pixels_metadata`.`DBS` (`DB_ID`)
                                                                 ON DELETE RESTRICT
                                                                 ON UPDATE CASCADE)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`RANGE_INDEXS`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`RANGE_INDEXS` (
                                                                `RI_ID` BIGINT NOT NULL AUTO_INCREMENT,
                                                                `RI_STRUCT` MEDIUMBLOB NOT NULL,
                                                                `KEY_COL_ID` BIGINT NOT NULL,
                                                                `TBLS_TBL_ID` BIGINT NOT NULL,
                                                                PRIMARY KEY (`RI_ID`),
                                                                INDEX `fk_RANGE_INDEX_TBLS_idx` (`TBLS_TBL_ID` ASC) VISIBLE,
                                                                UNIQUE INDEX `TBLS_TBL_ID_UNIQUE` (`TBLS_TBL_ID` ASC) VISIBLE,
                                                                INDEX `fk_RANGE_INDEX_COLS_idx` (`KEY_COL_ID` ASC) VISIBLE,
                                                                CONSTRAINT `fk_RANGE_INDEX_TBLS`
                                                                    FOREIGN KEY (`TBLS_TBL_ID`)
                                                                        REFERENCES `pixels_metadata`.`TBLS` (`TBL_ID`)
                                                                        ON DELETE RESTRICT
                                                                        ON UPDATE CASCADE,
                                                                CONSTRAINT `fk_RANGE_INDEX_COLS`
                                                                    FOREIGN KEY (`KEY_COL_ID`)
                                                                        REFERENCES `pixels_metadata`.`COLS` (`COL_ID`)
                                                                        ON DELETE RESTRICT
                                                                        ON UPDATE CASCADE)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`RANGES`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`RANGES` (
                                                          `RANGE_ID` BIGINT NOT NULL AUTO_INCREMENT,
                                                          `RANGE_RECORD_STATS` BLOB NOT NULL,
                                                          `RANGES_PARENT_ID` BIGINT NULL,
                                                          `RANGE_INDEXS_RI_ID` BIGINT NOT NULL,
                                                          PRIMARY KEY (`RANGE_ID`),
                                                          INDEX `fk_RANGES_RANGE_INDEXS_idx` (`RANGE_INDEXS_RI_ID` ASC) VISIBLE,
                                                          INDEX `fk_RANGES_RANGES_idx` (`RANGES_PARENT_ID` ASC) VISIBLE,
                                                          CONSTRAINT `fk_RANGES_RANGE_INDEXS`
                                                              FOREIGN KEY (`RANGE_INDEXS_RI_ID`)
                                                                  REFERENCES `pixels_metadata`.`RANGE_INDEXS` (`RI_ID`)
                                                                  ON DELETE RESTRICT
                                                                  ON UPDATE CASCADE,
                                                          CONSTRAINT `fk_RANGES_RANGES`
                                                              FOREIGN KEY (`RANGES_PARENT_ID`)
                                                                  REFERENCES `pixels_metadata`.`RANGES` (`RANGE_ID`)
                                                                  ON DELETE RESTRICT
                                                                  ON UPDATE CASCADE)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`PATHS`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`PATHS` (
                                                         `PATH_ID` BIGINT NOT NULL AUTO_INCREMENT,
                                                         `PATH_URI` VARCHAR(4096) NOT NULL,
                                                         `PATH_IS_COMPACT` TINYINT NOT NULL,
                                                         `LAYOUTS_LAYOUT_ID` BIGINT NOT NULL,
                                                         `RANGES_RANGE_ID` BIGINT NULL,
                                                         PRIMARY KEY (`PATH_ID`),
                                                         INDEX `fk_PATHS_RANGES_idx` (`RANGES_RANGE_ID` ASC) VISIBLE,
                                                         INDEX `fk_PATHS_LAYOUTS_idx` (`LAYOUTS_LAYOUT_ID` ASC) VISIBLE,
                                                         CONSTRAINT `fk_PATHS_RANGES`
                                                             FOREIGN KEY (`RANGES_RANGE_ID`)
                                                                 REFERENCES `pixels_metadata`.`RANGES` (`RANGE_ID`)
                                                                 ON DELETE RESTRICT
                                                                 ON UPDATE CASCADE,
                                                         CONSTRAINT `fk_PATHS_LAYOUTS`
                                                             FOREIGN KEY (`LAYOUTS_LAYOUT_ID`)
                                                                 REFERENCES `pixels_metadata`.`LAYOUTS` (`LAYOUT_ID`)
                                                                 ON DELETE RESTRICT
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
                                                         PRIMARY KEY (`USER_ID`))
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
                                                               INDEX `fk_USERS_has_DBS_DBS_idx` (`DBS_DB_ID` ASC) VISIBLE,
                                                               INDEX `fk_USERS_has_DBS_USERS_idx` (`USERS_USER_ID` ASC) VISIBLE,
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
                                                         `PEER_NAME` VARCHAR(128) NOT NULL,
                                                         `PEER_LOCATION` VARCHAR(1024) NOT NULL,
                                                         `PEER_HOST` VARCHAR(128) NOT NULL,
                                                         `PEER_PORT` INT NOT NULL,
                                                         `PEER_STORAGE_SCHEME` VARCHAR(32) NOT NULL,
                                                         PRIMARY KEY (`PEER_ID`))
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`PEER_PATHS`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `pixels_metadata`.`PEER_PATHS` (
                                                              `PEER_PATH_ID` BIGINT NOT NULL AUTO_INCREMENT,
                                                              `PEER_PATH_URI` VARCHAR(32) NOT NULL,
                                                              `PEER_PATH_COLUMNS` MEDIUMTEXT NOT NULL,
                                                              `PATHS_PATH_ID` BIGINT NOT NULL,
                                                              `PEERS_PEER_ID` BIGINT NOT NULL,
                                                              PRIMARY KEY (`PEER_PATH_ID`),
                                                              INDEX `fk_PEER_PATHS_PATHS_idx` (`PATHS_PATH_ID` ASC) VISIBLE,
                                                              INDEX `fk_PEER_PATHS_PEERS_idx` (`PEERS_PEER_ID` ASC) VISIBLE,
                                                              CONSTRAINT `fk_PEER_PATHS_PATHS`
                                                                  FOREIGN KEY (`PATHS_PATH_ID`)
                                                                      REFERENCES `pixels_metadata`.`PATHS` (`PATH_ID`)
                                                                      ON DELETE RESTRICT
                                                                      ON UPDATE CASCADE,
                                                              CONSTRAINT `fk_PEER_PATHS_PEERS`
                                                                  FOREIGN KEY (`PEERS_PEER_ID`)
                                                                      REFERENCES `pixels_metadata`.`PEERS` (`PEER_ID`)
                                                                      ON DELETE RESTRICT
                                                                      ON UPDATE CASCADE)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_bin;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
