package org.apache.hudi.commit.archer.configuration;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.HoodieConfig;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Hoodie Flink config options.
 *
 * <p>It has the options for Hoodie table read and write. It also defines some utilities.
 */
@ConfigClassProperty(name = "Archer Options",
    groupName = ConfigGroups.Names.FLINK_SQL,
    description = "Flink jobs using the SQL can be configured through the options in WITH clause."
        + " The actual datasource level configs are listed below.")
public class ArcherOptions extends HoodieConfig {
  // ------------------------------------------------------------------------
  //  archer commit Options
  // ------------------------------------------------------------------------
  public static final ConfigOption<Boolean> ARCHER_COMMIT_ENABLED = ConfigOptions
      .key("archer_commit.enabled")
      .booleanType()
      .defaultValue(false)
      .withDescription("Send message to archer, default false");

  public static final ConfigOption<String> ARCHER_COMMIT_TYPE = ConfigOptions
      .key("archer_commit.type")
      .stringType()
      .noDefaultValue()
      .withDescription("archer send message type, log_date or log_hour");

  public static final ConfigOption<String> ARCHER_COMMIT_USER = ConfigOptions
      .key("archer_commit.user")
      .stringType()
      .defaultValue("hudi")
      .withDescription("hive partition commit archer user,set conf by client");

  public static final ConfigOption<String> ARCHER_COMMIT_DB = ConfigOptions
      .key("archer_commit.db")
      .stringType()
      .defaultValue("")
      .withDescription("Database name for archer commit.");

  public static final ConfigOption<String> ARCHER_COMMIT_TABLE = ConfigOptions
      .key("archer_commit.table")
      .stringType()
      .defaultValue("")
      .withDescription("Table name for archer commit.");
}
