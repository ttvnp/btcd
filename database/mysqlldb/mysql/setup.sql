/*
 * CREATE DATABASE `mysqlldb_blocks` CHARACTER SET utf8;
 */

-- blocks
drop table if exists `blocks`;
create table `blocks` (
  `id` int(11) not null auto_increment,
  `network` int(11) not null,
  `block_len` int(11) not null,
  `checksum` varchar(8) character set utf8 not null default '',
  `raw_bytes` mediumblob not null,
  `hash` varchar(64) character set utf8 not null default '',
  `height` int(11) not null default 0,
  `block_time` timestamp not null default current_timestamp,
  `created` timestamp not null default current_timestamp,
  `updated` timestamp not null default current_timestamp on update current_timestamp,
  primary key (`id`),
  index idx_blocks_01 (`hash`),
  index idx_blocks_02 (`height`)
) engine=innodb default character set utf8;

-- transaction_outputs
drop table if exists `transaction_outputs`;
create table `transaction_outputs` (
  `id` int(11) not null auto_increment,
  `block_id` int(11) not null,
  `transaction_id` varchar(64) character set utf8 not null default '',
  `amount` bigint not null,
  `pk_script_bytes` mediumblob not null,
  `pk_script_class` tinyint unsigned not null,
  `created` timestamp not null default current_timestamp,
  `updated` timestamp not null default current_timestamp on update current_timestamp,
  primary key (`id`),
  index idx_transaction_outputs_01 (`block_id`),
  index idx_transaction_outputs_02 (`transaction_id`)
) engine=innodb default character set utf8;

-- transaction_output_addresses
drop table if exists `transaction_output_addresses`;
create table `transaction_output_addresses` (
  `id` int(11) not null auto_increment,
  `block_id` int(11) not null,
  `tx_out_id` int(11) not null,
  `address` varchar(255) character set utf8 not null default '',
  `created` timestamp not null default current_timestamp,
  `updated` timestamp not null default current_timestamp on update current_timestamp,
  primary key (`id`),
  index idx_transaction_output_addresses_01 (`address`)
) engine=innodb default character set utf8;
