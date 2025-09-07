package org.flywaydb.core;

import org.flywaydb.core.api.configuration.ClassicConfiguration;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.api.configuration.FluentConfiguration;

import java.util.Properties;

/**
 * @Author: BugMaker
 * @Date: 2025/9/7 23:10
 * @Description:
 **/
public class Test {
    public static void main(String[] args) {
        FluentConfiguration configure = Flyway.configure();

        Flyway load = Flyway.configure()
                .dataSource("jdbc:mysql://192.168.88.128:3306/flyway?characterEncoding=UTF-8&useUnicode=true&useSSL=false&tinyInt1isBit=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai", "root", "root")
                .load();
        load.migrate();
    }
}
