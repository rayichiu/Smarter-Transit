create external table subway_JZ (time INT, predictarrive INT, actualarrive INT, stopid STRING, subwayline STRING)
row format delimited fields terminated by ','
location '/user/hm2665/project/subway-hive-JZ';

SELECT min(predictarrive - actualarrive), percentile(predictarrive - actualarrive, 0.25), percentile(predictarrive - actualarrive, 0.5), percentile(predictarrive - actualarrive, 0.75), max(predictarrive - actualarrive) FROM subway_JZ;