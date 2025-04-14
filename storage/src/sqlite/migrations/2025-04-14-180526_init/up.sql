-- Your SQL goes here
CREATE TABLE `files`(
	`id` INTEGER NOT NULL PRIMARY KEY,
	`url` VARCHAR NOT NULL,
	`cache_path` VARCHAR UNIQUE NOT NULL,
	`filename` VARCHAR,
	`created` TIMESTAMPTZSQLITE NOT NULL,
	`last_used` TIMESTAMPTZSQLITE NOT NULL,
	`cache_policy` VARCHAR NOT NULL,
	`status` INTEGER NOT NULL,
	`ref_count` INTEGER NOT NULL
);

