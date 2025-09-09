/*
 * Copyright 2010-2020 Redgate Software Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core;

import org.flywaydb.core.api.configuration.ClassicConfiguration;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.api.configuration.FluentConfiguration;

import java.util.HashMap;
import java.util.Properties;

/**
 * @Author: BugMaker
 * @Date: 2025/9/7 23:10
 * @Description:
 **/
public class Test {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("flyway.cleanDisabled", "true");
        properties.setProperty("flyway.baselineOnMigrate", "true");

        Flyway load = Flyway.configure()
                .configuration(properties)
                .dataSource("jdbc:dm://192.168.1.243:5236?schema=OAP_TEST&clobAsString=1&localTimezone=Asia/Shanghai", "ORBIT", "OrBit@0408")
                .load();
        load.migrate();
    }
}
