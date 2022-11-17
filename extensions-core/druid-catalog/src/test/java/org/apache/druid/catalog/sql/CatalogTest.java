package org.apache.druid.catalog.sql;

import com.google.inject.Binder;
import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.ExternalTableBuilder;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.AbstractDatasourceDefn;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.ExternalTableDefn.FormattedExternalTableDefn;
import org.apache.druid.catalog.model.table.HttpTableDefn;
import org.apache.druid.catalog.model.table.InlineTableDefn;
import org.apache.druid.catalog.model.table.InputFormats;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.catalog.sync.CachedMetadataCatalog;
import org.apache.druid.catalog.sync.MetadataCatalog;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.sql.calcite.CalciteIngestionDmlTest;
import org.apache.druid.sql.calcite.CalciteInsertDmlTest;
import org.apache.druid.sql.calcite.external.ExternalOperatorConversion;
import org.apache.druid.sql.calcite.external.Externals;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.Builder;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.fail;

public class CatalogTest extends CalciteIngestionDmlTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private TableDefnRegistry registry;
  private CatalogTests.DbFixture dbFixture;
  private CatalogStorage storage;

  @Override
  protected void configureBuilder(Builder builder)
  {
    super.configureBuilder(builder);
    dbFixture = new CatalogTests.DbFixture(derbyConnectorRule);
    storage = dbFixture.storage;
    registry = new TableDefnRegistry(storage.jsonMapper());
    MetadataCatalog catalog = new CachedMetadataCatalog(
        storage,
        storage.schemaRegistry(),
        storage.jsonMapper()
    );
    builder.catalogResolver(new LiveCatalogResolver(catalog));
    builder.extraSchema(new ExternalSchema(catalog, storage.jsonMapper()));
  }

  /**
   * Test an inline table defined in the catalog. The structure is identical to the
   * inline tests in CatalogIngestionTest, only here the information comes from the
   * catalog.
   */
  @Test
  public void testInlineTable()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM ext.inline\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), Externals.externalRead("inline"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(CalciteInsertDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("insertFromExternal")
        .verify();
  }

  /**
   * Test an inline table defined in the catalog. The structure is identical to the
   * inline tests in CatalogIngestionTest, only here the information comes from the
   * catalog.
   */
  @Test
  public void testInlineTableFn()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(ext.inline())\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), Externals.externalRead("inline"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(CalciteInsertDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("insertFromExternal")
        .verify();
  }

  @After
  public void catalogTearDown()
  {
    CatalogTests.tearDown(dbFixture);
  }

  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    builder.addModule(new DruidModule() {

      @Override
      public void configure(Binder binder)
      {
        // Bindings, if such were possible. But, it isn't yet for tests.
        // binder.bind(MetadataCatalog.CatalogSource.class).toInstance(storage);
        // binder.bind(SchemaRegistry.class).toInstance(storage.schemaRegistry());
        // binder.bind(MetadataCatalog.class).to(CachedMetadataCatalog.class).in(LazySingleton.class);
        // binder.bind(CatalogResolver.class).to(LiveCatalogResolver.class).in(LazySingleton.class);

        // Register the external schema
        // SqlBindings.addSchema(binder, ExternalSchema.class);
      }
    });
  }

  @Override
  public void finalizeTestFramework(SqlTestFramework sqlTestFramework)
  {
    super.finalizeTestFramework(sqlTestFramework);
    try {
      buildInlineTable();
      buildKttmInputTable();
      buildTargetDatasources();
      buildFooDatasource();
    }
    catch (CatalogException e) {
      throw new ISE(e, e.getMessage());
    }
  }

  private void buildInlineTable() throws CatalogException
  {
    TableSpec inputDefn = new ExternalTableBuilder(registry.defnFor(InlineTableDefn.TABLE_TYPE))
        .property(FormattedExternalTableDefn.FORMAT_PROPERTY, InputFormats.CSV_FORMAT_TYPE)
        .property(InlineTableDefn.DATA_PROPERTY, Arrays.asList("a,b,1", "c,d,2"))
        .column("x", "VARCHAR")
        .column("y", "VARCHAR")
        .column("z", "BIGINT")
        .build();
    TableMetadata table = TableMetadata.newTable(
        TableId.external("inline"),
        inputDefn
    );
    storage.tables().create(table);
  }

  public void buildKttmInputTable() throws CatalogException
  {
    TableSpec inputDefn = new ExternalTableBuilder(registry.defnFor(HttpTableDefn.TABLE_TYPE))
        .property(FormattedExternalTableDefn.FORMAT_PROPERTY, InputFormats.JSON_FORMAT_TYPE)
        .property(HttpTableDefn.USER_PROPERTY, "vadim")
        .property(HttpTableDefn.PASSWORD_PROPERTY, "kangaroos_to_the_max")
        .column("timestamp", "VARCHAR")
        .column("agent_category", "VARCHAR")
        .column("agent_type", "VARCHAR")
        .column("browser", "VARCHAR")
        .column("browser_version", "VARCHAR")
        .column("city", "VARCHAR")
        .column("continent", "VARCHAR")
        .column("country", "VARCHAR")
        .column("version", "VARCHAR")
        .column("event_type", "VARCHAR")
        .column("event_subtype", "VARCHAR")
        .column("loaded_image", "VARCHAR")
        .column("adblock_list", "VARCHAR")
        .column("forwarded_for", "VARCHAR")
        .column("number", "BIGINT")
        .column("os", "VARCHAR")
        .column("path", "VARCHAR")
        .column("platform", "VARCHAR")
        .column("referrer", "VARCHAR")
        .column("referrer_host", "VARCHAR")
        .column("region", "VARCHAR")
        .column("remote_address", "VARCHAR")
        .column("screen", "VARCHAR")
        .column("session", "VARCHAR")
        .column("session_length", "VARCHAR")
        .column("timezone", "VARCHAR")
        .column("timezone_offset", "VARCHAR")
        .column("window", "VARCHAR")
        .build();
    TableMetadata table = TableMetadata.newTable(
        TableId.external("kttm_data"),
        inputDefn
    );
    storage.tables().create(table);
  }

  private void createTableMetadata(TableMetadata table)
  {
    try {
      storage.tables().create(table);
    } catch (CatalogException e) {
      fail(e.getMessage());
    }
  }

  private void buildTargetDatasources()
  {
    TableMetadata spec = TableBuilder.datasource("allDs", "ALL")
        .build();
    createTableMetadata(spec);

    spec = TableBuilder.datasource("hourDs", "PT1H")
        .build();
    createTableMetadata(spec);

    spec = TableBuilder.datasource("fiveMinDs", "PT5M")
        .property(AbstractDatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 5_432_198) // Easy to spot in results
        .build();
    createTableMetadata(spec);

    spec = TableBuilder.datasource("clusterBy", "P1D")
        .property(AbstractDatasourceDefn.CLUSTER_KEYS_PROPERTY, Arrays.asList(
            new ClusterKeySpec("floor_m1", false),
            new ClusterKeySpec("dim1", true)
         ))
        .build();
    createTableMetadata(spec);
  }

  public void buildFooDatasource()
  {
    TableMetadata spec = TableBuilder.datasource("foo", "ALL")
        .timeColumn()
        .column("extra1", null)
        .column("dim2", null)
        .column("dim1", null)
        .column("cnt", null)
        .column("m1", Columns.DOUBLE)
        .column("extra2", Columns.BIGINT)
        .column("extra3", Columns.VARCHAR)
        .hiddenColumns(Arrays.asList("dim3", "unique_dim1"))
        .build();
    createTableMetadata(spec);
  }
}
