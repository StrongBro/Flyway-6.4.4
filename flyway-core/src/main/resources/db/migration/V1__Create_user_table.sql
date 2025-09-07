CREATE TABLE `ACT_ID_USER` (
    `ID_` varchar(64) COLLATE utf8mb3_bin NOT NULL,
    `REV_` int DEFAULT NULL,
    `FIRST_` varchar(255) COLLATE utf8mb3_bin DEFAULT NULL,
    `LAST_` varchar(255) COLLATE utf8mb3_bin DEFAULT NULL,
    `DISPLAY_NAME_` varchar(255) COLLATE utf8mb3_bin DEFAULT NULL,
    `EMAIL_` varchar(255) COLLATE utf8mb3_bin DEFAULT NULL,
    `PWD_` varchar(255) COLLATE utf8mb3_bin DEFAULT NULL,
    `PICTURE_ID_` varchar(64) COLLATE utf8mb3_bin DEFAULT NULL,
    `TENANT_ID_` varchar(255) COLLATE utf8mb3_bin DEFAULT '',
    PRIMARY KEY (`ID_`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin;