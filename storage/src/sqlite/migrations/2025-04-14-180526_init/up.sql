-- Your SQL goes here
CREATE TABLE `files`(
	`id` INTEGER NOT NULL PRIMARY KEY,
	`source` VARCHAR NOT NULL,
	`cache_path` VARCHAR UNIQUE NOT NULL,
	`filename` VARCHAR,
	`created` TIMESTAMPTZSQLITE NOT NULL,
	`last_used` TIMESTAMPTZSQLITE NOT NULL,
	`store_policy` INTEGER NOT NULL,
	`store_policy_data` INTEGER,
	`status` INTEGER NOT NULL
);

