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
package org.flywaydb.core.internal.database.dm;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.SQLException;
import java.util.*;

import static org.flywaydb.core.internal.database.dm.DMSchema.ObjectType.*;

/**
 * DM implementation of Schema.
 */
public class DMSchema extends Schema<DMDatabase, DMTable> {
    private static final Log LOG = LogFactory.getLog(DMSchema.class);

    private static final String DM_TEMP_USER_SUFFIX = "_FLYWAY_USER";
    /**
     * Creates a new DM schema.
     *
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param database    The database-specific support.
     * @param name         The name of the schema.
     */
    DMSchema(JdbcTemplate jdbcTemplate, DMDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    /**
     * Checks whether the schema is system, i.e. DM-maintained, or not.
     *
     * @return {@code true} if it is system, {@code false} if not.
     */
    public boolean isSystem() throws SQLException {
        return database.getSystemSchemas().contains(name);
    }

    /**
     * Checks whether this schema is default for the current user.
     *
     * @return {@code true} if it is default, {@code false} if not.
     */
    boolean isDefaultSchemaForUser() throws SQLException {
        return name.equals(database.doGetCurrentUser());
    }

    @Override
    protected boolean doExists() throws SQLException {
        return database.queryReturnsRows("SELECT * FROM ALL_USERS WHERE USERNAME = ?", name);
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return !supportedTypesExist(jdbcTemplate, database, this);
    }

    @Override
    protected void doCreate() throws SQLException {
        System.out.println("CREATE USER " + database.quote(name) + " IDENTIFIED BY " + database.quote("FFllyywwaayy00!!"));
        jdbcTemplate.execute("CREATE USER " + database.quote(name + DM_TEMP_USER_SUFFIX) + " IDENTIFIED BY "
                + database.quote("FFllyywwaayy00!!"));
        jdbcTemplate.execute("GRANT RESOURCE TO " + database.quote(name));
        jdbcTemplate.execute("GRANT UNLIMITED TABLESPACE TO " + database.quote(name));
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP USER " + database.quote(name + DM_TEMP_USER_SUFFIX) + " CASCADE");
    }

    @Override
    protected void doClean() throws SQLException {
        if (isSystem()) {
            throw new FlywayException("Clean not supported on DM for system schema " + database.quote(name) + "! " +
                    "It must not be changed in any way except by running an DM-supplied script!");
        }

        // Disable FBA for schema tables.
        //达梦数据库的Flashback Archive功能不支持
//        if (database.isFlashbackDataArchiveAvailable()) {
//            disableFlashbackArchiveForFbaTrackedTables();
//        }

        // Clean DM Locator metadata.
//        if (database.isLocatorAvailable()) {
//            cleanLocatorMetadata();
//        }

        // Get existing object types in the schema.
        Set<String> objectTypeNames = getObjectTypeNames(jdbcTemplate, database, this);

        // Define the list of types to process, order is important.
        List<ObjectType> objectTypesToClean = Arrays.asList(
                TRIGGER,
                VIEW,
                TABLE,
                INDEX,
                SEQUENCE,
                PROCEDURE,
                FUNCTION,
                PACKAGE,
                DM_PACKAGE_BODY,
                TYPE,
                SYNONYM,
                DATABASE_LINK
        );

        for (ObjectType objectType : objectTypesToClean) {
            if (objectTypeNames.contains(objectType.getName())) {
                LOG.debug("Cleaning objects of type " + objectType + " ...");
                objectType.dropObjects(jdbcTemplate, database, this);
            }
        }

//        if (isDefaultSchemaForUser()) {
//            //oracle 清空回收站, 达梦不支持
//            jdbcTemplate.execute("PURGE RECYCLEBIN");
//        }
    }

    /**
     * Executes ALTER statements for all tables that have Flashback Archive enabled.
     * Flashback Archive is an asynchronous process so we need to wait until it completes, otherwise cleaning the
     * tables in schema will sometimes fail with ORA-55622 or ORA-55610 depending on the race between
     * Flashback Archive and Java code.
     *
     * @throws SQLException when the statements could not be generated.
     */
    private void disableFlashbackArchiveForFbaTrackedTables() throws SQLException {
        boolean dbaViewAccessible = database.isPrivOrRoleGranted("SELECT ANY DICTIONARY")
                || database.isDataDictViewAccessible("DBA_FLASHBACK_ARCHIVE_TABLES");

        if (!dbaViewAccessible && !isDefaultSchemaForUser()) {
            LOG.warn("Unable to check and disable Flashback Archive for tables in schema " + database.quote(name) +
                    " by user \"" + database.doGetCurrentUser() + "\": DBA_FLASHBACK_ARCHIVE_TABLES is not accessible");
            return;
        }

        boolean oracle18orNewer = database.getVersion().isAtLeast("18");

        String queryForFbaTrackedTables = "SELECT TABLE_NAME FROM " + (dbaViewAccessible ? "DBA_" : "USER_")
                + "FLASHBACK_ARCHIVE_TABLES WHERE OWNER_NAME = ?"
                + (oracle18orNewer ? " AND STATUS='ENABLED'" : "");
        List<String> tableNames = jdbcTemplate.queryForStringList(queryForFbaTrackedTables, name);
        for (String tableName : tableNames) {
            jdbcTemplate.execute("ALTER TABLE " + database.quote(name, tableName) + " NO FLASHBACK ARCHIVE");
            //wait until the tables disappear
            while (database.queryReturnsRows(queryForFbaTrackedTables + " AND TABLE_NAME = ?", name, tableName)) {
                try {
                    LOG.debug("Actively waiting for Flashback cleanup on table: " + database.quote(name, tableName));
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new FlywayException("Waiting for Flashback cleanup interrupted", e);
                }
            }
        }

        if (oracle18orNewer) {
            while (database.queryReturnsRows("SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = ?\n"
                    + " AND TABLE_NAME LIKE 'SYS_FBA_DDL_COLMAP_%'", name)) {
                try {
                    LOG.debug("Actively waiting for Flashback colmap cleanup");
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new FlywayException("Waiting for Flashback colmap cleanup interrupted", e);
                }
            }
        }
    }

    /**
     * Checks whether DM Locator metadata exists for the schema.
     *
     * @return {@code true} if it exists, {@code false} if not.
     * @throws SQLException when checking metadata existence failed.
     */
    private boolean locatorMetadataExists() throws SQLException {
        return database.queryReturnsRows("SELECT * FROM ALL_SDO_GEOM_METADATA WHERE OWNER = ?", name);
    }

    /**
     * Clean DM Locator metadata for the schema. Works only for the user's default schema, prints a warning message
     * to log otherwise.
     *
     * @throws SQLException when performing cleaning failed.
     */
    private void cleanLocatorMetadata() throws SQLException {
        if (!locatorMetadataExists()) {
            return;
        }

        if (!isDefaultSchemaForUser()) {
            LOG.warn("Unable to clean DM Locator metadata for schema " + database.quote(name) +
                    " by user \"" + database.doGetCurrentUser() + "\": unsupported operation");
            return;
        }

        jdbcTemplate.getConnection().commit();
        jdbcTemplate.execute("DELETE FROM USER_SDO_GEOM_METADATA");
        jdbcTemplate.getConnection().commit();
    }

    @Override
    protected DMTable[] doAllTables() throws SQLException {
        List<String> tableNames = TABLE.getObjectNames(jdbcTemplate, database, this);

        DMTable[] tables = new DMTable[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new DMTable(jdbcTemplate, database, this, tableNames.get(i));
        }
        return tables;
    }

    @Override
    public Table getTable(String tableName) {
        return new DMTable(jdbcTemplate, database, this, tableName);
    }


    /**
     * DM object types.
     */
    public enum ObjectType {
        // 表（达梦基础对象）
        TABLE("TABLE", "CASCADE CONSTRAINTS") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema) throws SQLException {
                // 简化查询，移除Oracle特有的逻辑
                return jdbcTemplate.queryForStringList(
                        "SELECT TABLE_NAME FROM DBA_TABLES WHERE OWNER = ?",
                        schema.getName()
                );
            }
        },

        // 索引（达梦支持）
        INDEX("INDEX") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                        "SELECT INDEX_NAME FROM DBA_INDEXES WHERE OWNER = ?",
                        schema.getName()
                );
            }
        },

        // 视图（达梦支持）
        VIEW("VIEW", "CASCADE CONSTRAINTS") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                        "SELECT VIEW_NAME FROM DBA_VIEWS WHERE OWNER = ?",
                        schema.getName()
                );
            }
        },

        // 序列（达梦支持）
        SEQUENCE("SEQUENCE") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                        "SELECT SEQUENCE_NAME FROM DBA_SEQUENCES WHERE SEQUENCE_OWNER = ?",
                        schema.getName()
                );
            }
        },

        // 存储过程（达梦支持）
        PROCEDURE("PROCEDURE") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                        "SELECT OBJECT_NAME FROM DBA_OBJECTS WHERE OWNER = ? AND OBJECT_TYPE = 'PROCEDURE'",
                        schema.getName()
                );
            }
        },

        // 函数（达梦支持）
        FUNCTION("FUNCTION") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                        "SELECT OBJECT_NAME FROM DBA_OBJECTS WHERE OWNER = ? AND OBJECT_TYPE = 'FUNCTION'",
                        schema.getName()
                );
            }
        },

        // 包（达梦支持）
        PACKAGE("PACKAGE") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                        "SELECT OBJECT_NAME FROM DBA_OBJECTS WHERE OWNER = ? AND OBJECT_TYPE = 'PACKAGE'",
                        schema.getName()
                );
            }
        },

        // 触发器（达梦支持）
        TRIGGER("TRIGGER") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                        "SELECT TRIGGER_NAME FROM DBA_TRIGGERS WHERE OWNER = ?",
                        schema.getName()
                );
            }
        },

        // 同义词（达梦支持）
        SYNONYM("SYNONYM") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                        "SELECT SYNONYM_NAME FROM DBA_SYNONYMS WHERE OWNER = ?",
                        schema.getName()
                );
            }
        },

        // 数据库链接（达梦支持DBLINK）
        DATABASE_LINK("DATABASE LINK") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                        "SELECT DB_LINK FROM DBA_DB_LINKS WHERE OWNER = ?",
                        schema.getName()
                );
            }

            @Override
            public String generateDropStatement(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema, String objectName) {
                return "DROP DATABASE LINK " + objectName;
            }
        },

        // 类型（达梦支持）
        TYPE("TYPE") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                        "SELECT OBJECT_NAME FROM DBA_OBJECTS WHERE OWNER = ? AND OBJECT_TYPE = 'TYPE'",
                        schema.getName()
                );
            }
        },

//        // 物化视图（达梦支持，但语法可能不同）
//        MATERIALIZED_VIEW("MATERIALIZED VIEW") {
//            @Override
//            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema) throws SQLException {
//                return jdbcTemplate.queryForStringList(
//                        "SELECT MVIEW_NAME FROM DBA_MVIEWS WHERE OWNER = ?",
//                        schema.getName()
//                );
//            }
//        },

        // 达梦特有的对象类型
        DM_PACKAGE_BODY("PACKAGE BODY") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                        "SELECT OBJECT_NAME FROM DBA_OBJECTS WHERE OWNER = ? AND OBJECT_TYPE = 'PACKAGE BODY'",
                        schema.getName()
                );
            }
        };

        /**
         * The name of the type as it mentioned in the Data Dictionary and the DROP statement.
         */
        private final String name;

        /**
         * The extra options used in the DROP statement to enforce the operation.
         */
        private final String dropOptions;

        ObjectType(String name, String dropOptions) {
            this.name = name;
            this.dropOptions = dropOptions;
        }

        ObjectType(String name) {
            this(name, "");
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return super.toString().replace('_', ' ');
        }

        /**
         * Returns the list of object names of this type.
         *
         * @throws SQLException if retrieving of objects failed.
         */
        public List<String> getObjectNames(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema) throws SQLException {
            return jdbcTemplate.queryForStringList(
                    "SELECT DISTINCT OBJECT_NAME FROM ALL_OBJECTS WHERE OWNER = ? AND OBJECT_TYPE = ?",
                    schema.getName(), this.getName()
            );
        }

        /**
         * Generates the drop statement for the specified object.
         *
         */
        public String generateDropStatement(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema, String objectName) {
            return "DROP " + this.getName() + " " + database.quote(schema.getName(), objectName) +
                    (StringUtils.hasText(dropOptions) ? " " + dropOptions : "");
        }

        /**
         * Drops all objects of this type in the specified schema.
         *
         * @throws SQLException if cleaning failed.
         */
        public void dropObjects(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema) throws SQLException {
            for (String objectName : getObjectNames(jdbcTemplate, database, schema)) {
                jdbcTemplate.execute(generateDropStatement(jdbcTemplate, database, schema, objectName));
            }
        }

        private void warnUnsupported(String schemaName, String typeDesc) {
            LOG.warn("Unable to clean " + typeDesc + " for schema " + schemaName + ": unsupported operation");
        }

        private void warnUnsupported(String schemaName) {
            warnUnsupported(schemaName, this.toString().toLowerCase() + "s");
        }

        /**
         * Returns the schema's existing object types.
         *
         * @return a set of object type names.
         * @throws SQLException if retrieving of object types failed.
         */
        public static Set<String> getObjectTypeNames(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema) throws SQLException {
            String query =
                // 主要对象类型
                "SELECT DISTINCT OBJECT_TYPE FROM ALL_OBJECTS WHERE OWNER = ? " +
                // 表
                "UNION SELECT 'TABLE' FROM DUAL WHERE EXISTS(" +
                "SELECT * FROM DBA_TABLES WHERE OWNER = ?) " +
                // 视图
                "UNION SELECT 'VIEW' FROM DUAL WHERE EXISTS(" +
                "SELECT * FROM DBA_VIEWS WHERE OWNER = ?) " +
                // 索引
                "UNION SELECT 'INDEX' FROM DUAL WHERE EXISTS(" +
                "SELECT * FROM DBA_INDEXES WHERE OWNER = ?) " +
                // 序列
                "UNION SELECT 'SEQUENCE' FROM DUAL WHERE EXISTS(" +
                "SELECT * FROM DBA_SEQUENCES WHERE SEQUENCE_OWNER = ?) " +
                // 存储过程
                "UNION SELECT 'PROCEDURE' FROM DUAL WHERE EXISTS(" +
                "SELECT * FROM DBA_PROCEDURES WHERE OWNER = ?) " +
                // 函数
                "UNION SELECT 'FUNCTION' FROM DUAL WHERE EXISTS(" +
                "SELECT * FROM DBA_PROCEDURES WHERE OWNER = ? AND OBJECT_TYPE = 'FUNCTION') " +
                // 包
                "UNION SELECT 'PACKAGE' FROM DUAL WHERE EXISTS(" +
                "SELECT * FROM DBA_PROCEDURES WHERE OWNER = ? AND OBJECT_TYPE = 'PACKAGE') " +
                // 触发器
                "UNION SELECT 'TRIGGER' FROM DUAL WHERE EXISTS(" +
                "SELECT * FROM DBA_TRIGGERS WHERE OWNER = ?) " +
                // 同义词
                "UNION SELECT 'SYNONYM' FROM DUAL WHERE EXISTS(" +
                "SELECT * FROM DBA_SYNONYMS WHERE OWNER = ?) " +
                // 数据库链接
                "UNION SELECT 'DATABASE LINK' FROM DUAL WHERE EXISTS(" +
                "SELECT * FROM DBA_DB_LINKS WHERE OWNER = ?)";

            int n = 11; // 参数数量
            String[] params = new String[n];
            Arrays.fill(params, schema.getName());

            return new HashSet<>(jdbcTemplate.queryForStringList(query, params));
        }

        /**
         * Checks whether the specified schema contains object types that can be cleaned.
         *
         * @return {@code true} if it contains, {@code false} if not.
         * @throws SQLException if retrieving of object types failed.
         */
        public static boolean supportedTypesExist(JdbcTemplate jdbcTemplate, DMDatabase database, DMSchema schema) throws SQLException {
            Set<String> existingTypeNames = new HashSet<>(getObjectTypeNames(jdbcTemplate, database, schema));

            // Remove unsupported types.
            existingTypeNames.removeAll(Collections.singletonList(DATABASE_LINK.getName()));

            return !existingTypeNames.isEmpty();
        }
    }
}