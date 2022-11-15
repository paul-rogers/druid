package org.apache.druid.catalog.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.ExternalTableBuilder;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.InlineTableDefn;
import org.apache.druid.catalog.model.table.InputFormats;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.catalog.model.table.AbstractDatasourceDefn;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.ExternalTableDefn.FormattedExternalTableDefn;
import org.apache.druid.catalog.model.table.HttpTableDefn;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.util.Arrays;

import static org.junit.Assert.fail;

public class CatalogTest extends BaseCalciteQueryTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private ObjectMapper jsonMapper = new ObjectMapper();
  private TableDefnRegistry registry = new TableDefnRegistry(jsonMapper);
  private CatalogTests.DbFixture dbFixture;
  private CatalogStorage storage;

  @Before
  public void setUp()
  {
    dbFixture = new CatalogTests.DbFixture(derbyConnectorRule);
    storage = dbFixture.storage;
  }

  @After
  public void tearDown()
  {
    CatalogTests.tearDown(dbFixture);
  }

  private PlannerFixture.Builder catalogBuilder(boolean useMsqe) throws IOException
  {
    PlannerFixture.Builder builder = standardBuilder()
        .defaultQueryPlanningContext(
            ImmutableMap.of(
              // Enable MSQE so we can capture the controller task
              TalariaParserUtils.CTX_MULTI_STAGE_QUERY,
              useMsqe,
              // Use a the same dummy ID to make text compares easier.
              BaseQuery.SQL_QUERY_ID,
              "dummyId"))

        // Ugly, but we won't use either the native query factory or the indexing
        // client: we really just want the part that does the translation, but that
        // is wrapped up in query routing.
        .withQueryMaker(om -> new ImplyQueryMakerFactory(
            // Note: can't use this query maker factor for non-MSQE queries.
            // We can't easily get at the actual query lifecycle factory in this
            // config: this code would have to be in the planner factory, or we'd
            //have to use Guice.
            null,
            null,
            om))

        // To allow access to external tables
        .withAuthResult(CalciteTests.SUPER_USER_AUTH_RESULT)

        // MSQE needs dynamic Jackson configuration for the indexing tuning types
        .withJacksonModules(new IndexingServiceTuningConfigModule().getJacksonModules());

    // Bug: we need to inject this value in the injector created by Guice per run,
    // but we need to create the schema here, with an injector not yet built.
    // Using the early-stage injector doesn't work: BaseCalciteQuery.setMapperInjectableValues
    // overwrites any injectables we create. Need to sort this out properly.
    //.withJacksonInjectable(HttpInputSourceConfig.class, new HttpInputSourceConfig(null));

    CachedMetadataCatalog catalog = new CachedMetadataCatalog(storage, storage.schemaRegistry());
    storage.register(catalog);

    InputTableMacro macro = new InputTableMacro(builder.jsonMapper(), dbFixture.externModel);
    InputSchema inputSchema = new InputSchema(catalog, builder.jsonMapper(), macro);
    try {
      buildInlineTable();
      buildKttmInputTable();
      buildTargetDatasources();
      buildFooDatasource();
    }
    catch (DuplicateKeyException e) {
      throw new ISE(e, e.getMessage());
    }
    builder.withSchema(inputSchema);
    builder.withCatalogResolver(new LiveCatalogResolver(catalog));
    return builder;
  }

  private void buildInlineTable() throws CatalogException
  {
    TableSpec inputDefn = new ExternalTableBuilder(registry.defnFor(InlineTableDefn.TABLE_TYPE))
        .property(FormattedExternalTableDefn.FORMAT_PROPERTY, InputFormats.CSV_FORMAT_TYPE)
        .property(InlineTableDefn.DATA_PROPERTY, Arrays.asList("a,b,1\n", "c,d,2\n"))
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
    DatasourceSpec spec = DatasourceSpec
        .builder()
        .segmentGranularity("ALL")
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
    createTableMetadata("foo", spec);
  }
}
