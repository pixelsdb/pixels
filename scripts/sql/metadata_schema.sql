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
CREATE SCHEMA IF NOT EXISTS `pixels_metadata` DEFAULT CHARACTER SET utf8 COLLATE utf8_bin ;
USE `pixels_metadata` ;

-- -----------------------------------------------------
-- Table `pixels_metadata`.`DBS`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pixels_metadata`.`DBS` ;

CREATE TABLE IF NOT EXISTS `pixels_metadata`.`DBS` (
  `DB_ID` INT NOT NULL AUTO_INCREMENT,
  `DB_NAME` VARCHAR(128) NOT NULL,
  `DB_DESC` VARCHAR(4000) NULL,
  PRIMARY KEY (`DB_ID`),
  UNIQUE INDEX `DB_NAME_UNIQUE` (`DB_NAME` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`TBLS`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pixels_metadata`.`TBLS` ;

CREATE TABLE IF NOT EXISTS `pixels_metadata`.`TBLS` (
  `TBL_ID` INT NOT NULL AUTO_INCREMENT,
  `TBL_NAME` VARCHAR(128) NOT NULL,
  `TBL_TYPE` VARCHAR(128) NULL,
  `TBL_STORAGE_SCHEME` VARCHAR(32) NOT NULL DEFAULT 'hdfs',
  `DBS_DB_ID` INT NOT NULL,
  PRIMARY KEY (`TBL_ID`),
  INDEX `fk_TBLS_DBS_idx` (`DBS_DB_ID` ASC),
  UNIQUE INDEX `TBL_NAME_DB_ID_UNIQUE` (`TBL_NAME` ASC, `DBS_DB_ID` ASC),
  CONSTRAINT `fk_TBLS_DBS`
    FOREIGN KEY (`DBS_DB_ID`)
    REFERENCES `pixels_metadata`.`DBS` (`DB_ID`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`COLS`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pixels_metadata`.`COLS` ;

CREATE TABLE IF NOT EXISTS `pixels_metadata`.`COLS` (
  `COL_ID` INT NOT NULL AUTO_INCREMENT,
  `COL_NAME` VARCHAR(128) NOT NULL,
  `COL_TYPE` VARCHAR(128) NOT NULL,
  `COL_SIZE` DOUBLE NOT NULL,
  `TBLS_TBL_ID` INT NOT NULL,
  PRIMARY KEY (`COL_ID`),
  INDEX `fk_COLS_TBLS1_idx` (`TBLS_TBL_ID` ASC),
  UNIQUE INDEX `COL_NAME_TBL_ID_UNIQUE` (`COL_NAME` ASC, `TBLS_TBL_ID` ASC),
  CONSTRAINT `fk_COLS_TBLS1`
    FOREIGN KEY (`TBLS_TBL_ID`)
    REFERENCES `pixels_metadata`.`TBLS` (`TBL_ID`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`LAYOUTS`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pixels_metadata`.`LAYOUTS` ;

CREATE TABLE IF NOT EXISTS `pixels_metadata`.`LAYOUTS` (
  `LAYOUT_ID` INT NOT NULL AUTO_INCREMENT,
  `LAYOUT_VERSION` INT NOT NULL COMMENT 'The version of this layout.',
  `LAYOUT_CREATE_AT` BIGINT NOT NULL COMMENT 'The time (output of System.currentTimeMillis()) on which this layout is created.',
  `LAYOUT_PERMISSION` TINYINT NOT NULL COMMENT '<0 for not readable and writable, 0 for readable only, >0 for readable and writable.',
  `LAYOUT_ORDER` MEDIUMTEXT NOT NULL COMMENT 'The default order of this layout. It is used to determine the column order in a single-row-group blocks.',
  `LAYOUT_ORDER_PATH` VARCHAR(4000) NOT NULL COMMENT 'The directory where single-row-group files are initially stored.',
  `LAYOUT_COMPACT` LONGTEXT NOT NULL COMMENT 'the layout strategy, stored as json. It is used to determine how row groups are compacted into a big block.',
  `LAYOUT_COMPACT_PATH` VARCHAR(4000) NOT NULL COMMENT 'The path where compacted big files are stored.',
  `LAYOUT_SPLITS` LONGTEXT NOT NULL COMMENT 'The suggested split size for access patterns, stored as json.',
  `LAYOUT_PROJECTIONS` LONGTEXT NOT NULL,
  `TBLS_TBL_ID` INT NOT NULL,
  PRIMARY KEY (`LAYOUT_ID`),
  INDEX `fk_LAYOUT_TBLS1_idx` (`TBLS_TBL_ID` ASC) ,
  CONSTRAINT `fk_LAYOUT_TBLS1`
    FOREIGN KEY (`TBLS_TBL_ID`)
    REFERENCES `pixels_metadata`.`TBLS` (`TBL_ID`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_bin;


-- -----------------------------------------------------
-- Table `pixels_metadata`.`VIEWS`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pixels_metadata`.`VIEWS` ;

CREATE TABLE IF NOT EXISTS `pixels_metadata`.`VIEWS` (
  `VIEW_ID` INT NOT NULL AUTO_INCREMENT,
  `VIEW_NAME` VARCHAR(128) NOT NULL,
  `VIEW_TYPE` VARCHAR(128) NULL,
  `VIEW_DATA` LONGTEXT NOT NULL,
  `DBS_DB_ID` INT NOT NULL,
  PRIMARY KEY (`VIEW_ID`),
  INDEX `fk_VIEWS_DBS1_idx` (`DBS_DB_ID` ASC) ,
  CONSTRAINT `fk_VIEWS_DBS1`
    FOREIGN KEY (`DBS_DB_ID`)
    REFERENCES `pixels_metadata`.`DBS` (`DB_ID`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_bin;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

