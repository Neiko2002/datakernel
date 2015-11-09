CREATE TABLE `aggregation_db_chunk` (
  `id` BIGINT(20) NOT NULL AUTO_INCREMENT,
  `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `aggregation_id` VARCHAR(100) NOT NULL DEFAULT '',
  `revision_id` INT(11) NOT NULL,
  `count` INT(11) NOT NULL,
  `consolidated_revision_id` INT(11) NULL DEFAULT NULL,
  `consolidation_started` TIMESTAMP NULL DEFAULT NULL,
  `consolidation_completed` TIMESTAMP NULL DEFAULT NULL,
  `keys` VARCHAR(1000) NOT NULL DEFAULT '',
  `fields` VARCHAR(1000) NOT NULL DEFAULT '',
  `d1_min` TEXT NULL DEFAULT NULL,
  `d1_max` TEXT NULL DEFAULT NULL,
  `d2_min` TEXT NULL DEFAULT NULL,
  `d2_max` TEXT NULL DEFAULT NULL,
  `d3_min` TEXT NULL DEFAULT NULL,
  `d3_max` TEXT NULL DEFAULT NULL,
  `d4_min` TEXT NULL DEFAULT NULL,
  `d4_max` TEXT NULL DEFAULT NULL,
  `d5_min` TEXT NULL DEFAULT NULL,
  `d5_max` TEXT NULL DEFAULT NULL,
  `d6_min` TEXT NULL DEFAULT NULL,
  `d6_max` TEXT NULL DEFAULT NULL,
  `d7_min` TEXT NULL DEFAULT NULL,
  `d7_max` TEXT NULL DEFAULT NULL,
  `d8_min` TEXT NULL DEFAULT NULL,
  `d8_max` TEXT NULL DEFAULT NULL,
  `d9_min` TEXT NULL DEFAULT NULL,
  `d9_max` TEXT NULL DEFAULT NULL,
  `d10_min` TEXT NULL DEFAULT NULL,
  `d10_max` TEXT NULL DEFAULT NULL,
  `d11_min` TEXT NULL DEFAULT NULL,
  `d11_max` TEXT NULL DEFAULT NULL,
  `d12_min` TEXT NULL DEFAULT NULL,
  `d12_max` TEXT NULL DEFAULT NULL,
  `d13_min` TEXT NULL DEFAULT NULL,
  `d13_max` TEXT NULL DEFAULT NULL,
  `d14_min` TEXT NULL DEFAULT NULL,
  `d14_max` TEXT NULL DEFAULT NULL,
  `d15_min` TEXT NULL DEFAULT NULL,
  `d15_max` TEXT NULL DEFAULT NULL,
  `d16_min` TEXT NULL DEFAULT NULL,
  `d16_max` TEXT NULL DEFAULT NULL,
  `d17_min` TEXT NULL DEFAULT NULL,
  `d17_max` TEXT NULL DEFAULT NULL,
  `d18_min` TEXT NULL DEFAULT NULL,
  `d18_max` TEXT NULL DEFAULT NULL,
  `d19_min` TEXT NULL DEFAULT NULL,
  `d19_max` TEXT NULL DEFAULT NULL,
  `d20_min` TEXT NULL DEFAULT NULL,
  `d20_max` TEXT NULL DEFAULT NULL,
  `d21_min` TEXT NULL DEFAULT NULL,
  `d21_max` TEXT NULL DEFAULT NULL,
  `d22_min` TEXT NULL DEFAULT NULL,
  `d22_max` TEXT NULL DEFAULT NULL,
  `d23_min` TEXT NULL DEFAULT NULL,
  `d23_max` TEXT NULL DEFAULT NULL,
  `d24_min` TEXT NULL DEFAULT NULL,
  `d24_max` TEXT NULL DEFAULT NULL,
  `d25_min` TEXT NULL DEFAULT NULL,
  `d25_max` TEXT NULL DEFAULT NULL,
  `d26_min` TEXT NULL DEFAULT NULL,
  `d26_max` TEXT NULL DEFAULT NULL,
  `d27_min` TEXT NULL DEFAULT NULL,
  `d27_max` TEXT NULL DEFAULT NULL,
  `d28_min` TEXT NULL DEFAULT NULL,
  `d28_max` TEXT NULL DEFAULT NULL,
  `d29_min` TEXT NULL DEFAULT NULL,
  `d29_max` TEXT NULL DEFAULT NULL,
  `d30_min` TEXT NULL DEFAULT NULL,
  `d30_max` TEXT NULL DEFAULT NULL,
  `d31_min` TEXT NULL DEFAULT NULL,
  `d31_max` TEXT NULL DEFAULT NULL,
  `d32_min` TEXT NULL DEFAULT NULL,
  `d32_max` TEXT NULL DEFAULT NULL,
  `d33_min` TEXT NULL DEFAULT NULL,
  `d33_max` TEXT NULL DEFAULT NULL,
  `d34_min` TEXT NULL DEFAULT NULL,
  `d34_max` TEXT NULL DEFAULT NULL,
  `d35_min` TEXT NULL DEFAULT NULL,
  `d35_max` TEXT NULL DEFAULT NULL,
  `d36_min` TEXT NULL DEFAULT NULL,
  `d36_max` TEXT NULL DEFAULT NULL,
  `d37_min` TEXT NULL DEFAULT NULL,
  `d37_max` TEXT NULL DEFAULT NULL,
  `d38_min` TEXT NULL DEFAULT NULL,
  `d38_max` TEXT NULL DEFAULT NULL,
  `d39_min` TEXT NULL DEFAULT NULL,
  `d39_max` TEXT NULL DEFAULT NULL,
  `d40_min` TEXT NULL DEFAULT NULL,
  `d40_max` TEXT NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET=utf8;