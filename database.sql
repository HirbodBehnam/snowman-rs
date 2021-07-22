CREATE
DATABASE `snowman`;
-- This table contains current balances of all users
CREATE TABLE `snowman`.`current_balance`
(
    `user_id`  INT UNSIGNED NOT NULL,
    `balances` JSON NOT NULL DEFAULT (JSON_OBJECT()) COMMENT 'An object in form of currency -> volume of user',
    PRIMARY KEY (`user_id`)
) ENGINE = InnoDB;
-- In this table, we hold the past balances of users
CREATE TABLE `snowman`.`past_balance`
(
    `id`       INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `user_id`  INT UNSIGNED NOT NULL,
    `balances` JSON   NOT NULL DEFAULT (JSON_OBJECT()) COMMENT 'An object in form of currency -> volume of user',
    `changed`  BIGINT NOT NULL COMMENT 'Record added time in unix epoch',
    PRIMARY KEY (`id`),
    INDEX (`user_id`),
    INDEX (`changed`)
) ENGINE = InnoDB;