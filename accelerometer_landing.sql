CREATE EXTERNAL TABLE IF NOT EXISTS `wyatt-udacity-db`.`accelerometer_landing` (
  `user` string,
  `timeStamp` timestamp,
  `x` smallint,
  `y` smallint,
  `z` smallint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://wyatt-7654/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');