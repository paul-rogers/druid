/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceWrapper;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.queryng.fragment.FragmentBuilder;
import org.apache.druid.queryng.fragment.FragmentBuilderFactory;
import org.apache.druid.queryng.fragment.FragmentHandle;
import org.apache.druid.queryng.fragment.Fragments;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.server.QueryResource.ResourceIOReaderWriter;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Class that helps a Druid server (broker, historical, etc) manage the lifecycle of a query that it is handling. It
 * ensures that a query goes through the following stages, in the proper order:
 *
 * <ol>
 * <li>Initialization ({@link #initialize(Query)})</li>
 * <li>Authorization ({@link #authorize(HttpServletRequest)}</li>
 * <li>Execution ({@link #execute()}</li>
 * <li>Logging ({@link #emitLogsAndMetrics(Throwable, String, long)}</li>
 * </ol>
 *
 * This object is not thread-safe.
 */
public class QueryLifecycle
{
  private static final Logger log = new Logger(QueryLifecycle.class);

  private final QueryToolChestWarehouse warehouse;
  private final QuerySegmentWalker texasRanger;
  private final GenericQueryMetricsFactory queryMetricsFactory;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final AuthorizerMapper authorizerMapper;
  private final DefaultQueryConfig defaultQueryConfig;
  private final AuthConfig authConfig;
  private final FragmentBuilderFactory fragmentContextFactory;
  private final long startMs;
  private final long startNs;

  private State state = State.NEW;
  private AuthenticationResult authenticationResult;
  private QueryToolChest toolChest;

  @MonotonicNonNull
  private Query<?> baseQuery;

  public QueryLifecycle(
      final QueryToolChestWarehouse warehouse,
      final QuerySegmentWalker texasRanger,
      final GenericQueryMetricsFactory queryMetricsFactory,
      final ServiceEmitter emitter,
      final RequestLogger requestLogger,
      final AuthorizerMapper authorizerMapper,
      final DefaultQueryConfig defaultQueryConfig,
      final AuthConfig authConfig,
      final FragmentBuilderFactory fragmentContextFactory,
      final long startMs,
      final long startNs
  )
  {
    this.warehouse = warehouse;
    this.texasRanger = texasRanger;
    this.queryMetricsFactory = queryMetricsFactory;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.authorizerMapper = authorizerMapper;
    this.defaultQueryConfig = defaultQueryConfig;
    this.authConfig = authConfig;
    this.fragmentContextFactory = fragmentContextFactory;
    this.startMs = startMs;
    this.startNs = startNs;
  }

  /**
   * For callers who have already authorized their query, and where simplicity is desired over flexibility. This method
   * does it all in one call. Logs and metrics are emitted when the Sequence is either fully iterated or throws an
   * exception.
   *
   * @param query                 the query
   * @param authenticationResult  authentication result indicating identity of the requester
   * @param authorizationResult   authorization result of requester
   *
   * @return results
   */
  public <T> Sequence<T> runSimple(
      final Query<T> query,
      final AuthenticationResult authenticationResult,
      final Access authorizationResult
  )
  {
    return runSimpleWithResponse(query, authenticationResult, authorizationResult).getResults();
  }

  /**
   * Runs the query directly when the query is part of a query chain, such as
   * when being run from SQL.
   *
   * @return a {@link QueryResponse} that provides both the Sequence and
   * operator form of the query. The Sequence form is always available; the operator
   * form is available if operators are enabled for this particular query.
   */
  public <T> QueryResponse<T> runSimpleWithResponse(
      final Query<T> query,
      final AuthenticationResult authenticationResult,
      final Access authorizationResult
  )
  {
    initialize(query);

    try {
      preAuthorized(authenticationResult, authorizationResult);
      if (!authorizationResult.isAllowed()) {
        throw new ISE("Unauthorized");
      }

      return execute();
    }
    catch (Throwable e) {
      emitLogsAndMetrics(e, null, -1);
      throw e;
    }
  }

  /**
   * Initializes this object to execute a specific query. Does not actually execute the query.
   *
   * @param baseQuery the query
   */
  public void initialize(final Query<?> baseQuery)
  {
    transition(State.NEW, State.INITIALIZED);

    baseQuery.getQueryContext().addDefaultParam(BaseQuery.QUERY_ID, UUID.randomUUID().toString());
    baseQuery.getQueryContext().addDefaultParams(defaultQueryConfig.getContext());

    this.baseQuery = baseQuery;
    this.toolChest = warehouse.getToolChest(baseQuery);
  }

  /**
   * Authorize the query. Will return an Access object denoting whether the query is authorized or not.
   *
   * @param req HTTP request object of the request. If provided, the auth-related fields in the HTTP request
   *            will be automatically set.
   *
   * @return authorization result
   */
  public Access authorize(HttpServletRequest req)
  {
    transition(State.INITIALIZED, State.AUTHORIZING);
    final Iterable<ResourceAction> resourcesToAuthorize = Iterables.concat(
        Iterables.transform(
            baseQuery.getDataSource().getTableNames(),
            AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR
        ),
        authConfig.authorizeQueryContextParams()
        ? Iterables.transform(
            baseQuery.getQueryContext().getUserParams().keySet(),
            contextParam -> new ResourceAction(new Resource(contextParam, ResourceType.QUERY_CONTEXT), Action.WRITE)
        )
        : Collections.emptyList()
    );
    return doAuthorize(
        AuthorizationUtils.authenticationResultFromRequest(req),
        AuthorizationUtils.authorizeAllResourceActions(
            req,
            resourcesToAuthorize,
            authorizerMapper
        )
    );
  }

  private void preAuthorized(final AuthenticationResult authenticationResult, final Access access)
  {
    // gotta transition those states, even if we are already authorized
    transition(State.INITIALIZED, State.AUTHORIZING);
    doAuthorize(authenticationResult, access);
  }

  private Access doAuthorize(final AuthenticationResult authenticationResult, final Access authorizationResult)
  {
    Preconditions.checkNotNull(authenticationResult, "authenticationResult");
    Preconditions.checkNotNull(authorizationResult, "authorizationResult");

    if (!authorizationResult.isAllowed()) {
      // Not authorized; go straight to Jail, do not pass Go.
      transition(State.AUTHORIZING, State.UNAUTHORIZED);
    } else {
      transition(State.AUTHORIZING, State.AUTHORIZED);
    }

    this.authenticationResult = authenticationResult;

    return authorizationResult;
  }

  /**
   * Execute the query. Can only be called if the query has been authorized. Note that query logs and metrics will
   * not be emitted automatically when the Sequence is fully iterated. It is the caller's responsibility to call
   * {@link #emitLogsAndMetrics(Throwable, String, long)} to emit logs and metrics.
   *
   * @return result sequence and response context
   */
  public <T> QueryResponse<T> execute()
  {
    transition(State.AUTHORIZED, State.EXECUTING);

    final ResponseContext responseContext = DirectDruidClient.makeResponseContextForQuery();

    final FragmentBuilder fragmentBuilder = fragmentContextFactory.create(baseQuery, responseContext);
    @SuppressWarnings("unchecked")
    final Sequence<T> res = (Sequence<T>) QueryPlus.wrap(baseQuery)
                                  .withIdentity(authenticationResult.getIdentity())
                                  .withFragmentBuilder(fragmentBuilder)
                                  .run(texasRanger, responseContext);

    if (fragmentBuilder == null) {
      Sequence<T> wrapped = Sequences.wrap(
          res,
          new SequenceWrapper()
          {
            @Override
            public void after(final boolean isDone, final Throwable thrown)
            {
              emitLogsAndMetrics(thrown, null, -1);
            }
          }
      );
      return new SequenceResponse<>(responseContext, wrapped);
    } else {
      // Operator version of the above.
      // TODO: Move to an actual operator class, which will require refactoring
      // the emitLogsAndMetrics method.
      fragmentBuilder.context().onClose(context -> {
          emitLogsAndMetrics(context.exception(), null, -1);
        }
      );
      fragmentBuilder.context().onClose(context -> {
          Fragments.logProfile(context);
        }
      );
      return new FragmentResponse<>(responseContext, fragmentBuilder.handle(res));
    }
  }

  /**
   * Emit logs and metrics for this query.
   *
   * @param e             exception that occurred while processing this query
   * @param remoteAddress remote address, for logging; or null if unknown
   * @param bytesWritten  number of bytes written; will become a query/bytes metric if >= 0
   */
  @SuppressWarnings("unchecked")
  public void emitLogsAndMetrics(
      @Nullable final Throwable e,
      @Nullable final String remoteAddress,
      final long bytesWritten
  )
  {
    if (baseQuery == null) {
      // Never initialized, don't log or emit anything.
      return;
    }

    if (state == State.DONE) {
      log.warn("Tried to emit logs and metrics twice for query [%s]!", baseQuery.getId());
    }

    state = State.DONE;

    final boolean success = e == null;

    try {
      final long queryTimeNs = System.nanoTime() - startNs;

      QueryMetrics<?> queryMetrics = DruidMetrics.makeRequestMetrics(
          queryMetricsFactory,
          toolChest,
          baseQuery,
          StringUtils.nullToEmptyNonDruidDataString(remoteAddress)
      );
      queryMetrics.success(success);
      queryMetrics.reportQueryTime(queryTimeNs);

      if (bytesWritten >= 0) {
        queryMetrics.reportQueryBytes(bytesWritten);
      }

      if (authenticationResult != null) {
        queryMetrics.identity(authenticationResult.getIdentity());
      }

      queryMetrics.emit(emitter);

      final Map<String, Object> statsMap = new LinkedHashMap<>();
      statsMap.put("query/time", TimeUnit.NANOSECONDS.toMillis(queryTimeNs));
      statsMap.put("query/bytes", bytesWritten);
      statsMap.put("success", success);

      if (authenticationResult != null) {
        statsMap.put("identity", authenticationResult.getIdentity());
      }

      if (e != null) {
        statsMap.put("exception", e.toString());
        if (QueryContexts.isDebug(baseQuery)) {
          log.warn(e, "Exception while processing queryId [%s]", baseQuery.getId());
        } else {
          log.noStackTrace().warn(e, "Exception while processing queryId [%s]", baseQuery.getId());
        }
        if (e instanceof QueryInterruptedException || e instanceof QueryTimeoutException) {
          // Mimic behavior from QueryResource, where this code was originally taken from.
          statsMap.put("interrupted", true);
          statsMap.put("reason", e.toString());
        }
      }

      requestLogger.logNativeQuery(
          RequestLogLine.forNative(
              baseQuery,
              DateTimes.utc(startMs),
              StringUtils.nullToEmptyNonDruidDataString(remoteAddress),
              new QueryStats(statsMap)
          )
      );
    }
    catch (Exception ex) {
      log.error(ex, "Unable to log query [%s]!", baseQuery);
    }
  }

  @Nullable
  public Query<?> getQuery()
  {
    return baseQuery;
  }

  public String getQueryId()
  {
    return baseQuery.getId();
  }

  public String threadName(String currThreadName)
  {
    return StringUtils.format(
        "%s[%s_%s_%s]",
        currThreadName,
        baseQuery.getType(),
        baseQuery.getDataSource().getTableNames(),
        getQueryId()
    );
  }

  private boolean isSerializeDateTimeAsLong()
  {
    final boolean shouldFinalize = QueryContexts.isFinalize(baseQuery, true);
    return QueryContexts.isSerializeDateTimeAsLong(baseQuery, false)
           || (!shouldFinalize && QueryContexts.isSerializeDateTimeAsLongInner(baseQuery, false));
  }

  public ObjectWriter newOutputWriter(ResourceIOReaderWriter ioReaderWriter)
  {
    return ioReaderWriter.getResponseWriter().newOutputWriter(
        getToolChest(),
        baseQuery,
        isSerializeDateTimeAsLong()
    );
  }

  public QueryToolChest getToolChest()
  {
    if (state.compareTo(State.INITIALIZED) < 0) {
      throw new ISE("Not yet initialized");
    }

    //noinspection unchecked
    return toolChest;
  }

  private void transition(final State from, final State to)
  {
    if (state != from) {
      throw new ISE("Cannot transition from [%s] to [%s].", from, to);
    }

    state = to;
  }

  enum State
  {
    NEW,
    INITIALIZED,
    AUTHORIZING,
    AUTHORIZED,
    EXECUTING,
    UNAUTHORIZED,
    DONE
  }

  public abstract static class QueryResponse<T>
  {
    private final ResponseContext responseContext;

    private QueryResponse(
        final ResponseContext responseContext
    )
    {
      this.responseContext = responseContext;
    }

    public abstract boolean isFragment();

    public ResponseContext getResponseContext()
    {
      return responseContext;
    }

    public abstract Sequence<T> getResults();

    public FragmentHandle<T> fragmentHandle()
    {
      return null;
    }

    public FragmentBuilder fragmentBuilder()
    {
      return null;
    }

    public <U> QueryResponse<U> withSequence(final Sequence<U> results)
    {
      throw new UOE("withSequence");
    }

    public <U> QueryResponse<U> withRoot(final Operator<U> root)
    {
      throw new UOE("withRoot");
    }

  }

  public static class FragmentResponse<T> extends QueryResponse<T>
  {
    /**
     * Fragment context is included because the SQL layer adds another
     * operator on top of the sequence returned here, and that operator
     * (if enabled), needs visibility to the fragment context.
     */
    private final FragmentHandle<T> fragmentHandle;

    private FragmentResponse(
        final ResponseContext responseContext,
        final FragmentHandle<T> fragmentHandle
    )
    {
      super(responseContext);
      this.fragmentHandle = fragmentHandle;
    }

    @Override
    public boolean isFragment()
    {
      return true;
    }

    @Override
    public Sequence<T> getResults()
    {
      return fragmentHandle.runAsSequence();
    }

    @Override
    public FragmentHandle<T> fragmentHandle()
    {
      return fragmentHandle;
    }

    @Override
    public FragmentBuilder fragmentBuilder()
    {
      return fragmentHandle.builder();
    }

    @Override
    public <U> QueryResponse<U> withRoot(final Operator<U> root)
    {
      return new FragmentResponse<U>(getResponseContext(), fragmentHandle.compose(root));
    }
  }

  public static class SequenceResponse<T> extends QueryResponse<T>
  {
    private final Sequence<T> results;

    private SequenceResponse(
        final ResponseContext responseContext,
        final Sequence<T> results
    )
    {
      super(responseContext);
      this.results = results == null ? Sequences.empty() : results;
    }

    @Override
    public boolean isFragment()
    {
      return false;
    }

    @Override
    public Sequence<T> getResults()
    {
      return results;
    }

    @Override
    public <U> QueryResponse<U> withSequence(final Sequence<U> results)
    {
      return new SequenceResponse<U>(getResponseContext(), results);
    }
  }

//  public static class QueryResponse<T>
//  {
//    private final Sequence<T> results;
//    private final ResponseContext responseContext;
//
//    /**
//     * Fragment context is included because the SQL layer adds another
//     * operator on top of the sequence returned here, and that operator
//     * (if enabled), needs visibility to the fragment context.
//     */
//    private final FragmentHandle<T> fragmentHandle;
//
//    private QueryResponse(
//        final Sequence<T> results,
//        final ResponseContext responseContext,
//        final FragmentBuilder fragmentBuilder)
//    {
//      this(
//          results == null ? Sequences.empty() : results,
//          responseContext,
//          fragmentBuilder == null || results == null
//            ? null
//            : fragmentBuilder.handle(results)
//      );
//    }
//
//    private QueryResponse(
//        final Sequence<T> results,
//        final ResponseContext responseContext,
//        final FragmentHandle<T> fragmentHandle)
//    {
//      this.results = results == null ? Sequences.empty() : results;
//      this.responseContext = responseContext;
//      this.fragmentHandle = fragmentHandle;
//    }
//
//    public <U> QueryResponse<U> withSequence(final Sequence<U> results)
//    {
//      return new QueryResponse<U>(
//          results,
//          responseContext,
//          fragmentHandle == null ? null : fragmentHandle.compose(results));
//    }
//
//    public <U> QueryResponse<U> withRoot(final Operator<U> root)
//    {
//      Sequence<U> rootSeq = Operators.toSequence(root);
//      return new QueryResponse<U>(
//          rootSeq,
//          responseContext,
//          fragmentHandle.compose(rootSeq));
//    }
//
//    public Sequence<T> getResults()
//    {
//      return results;
//    }
//
//    public ResponseContext getResponseContext()
//    {
//      return responseContext;
//    }
//
//    public FragmentHandle<T> fragmentHandle()
//    {
//      return fragmentHandle;
//    }
//
//    public FragmentBuilder fragmentBuilder()
//    {
//      return fragmentHandle == null ? null : fragmentHandle.builder();
//    }
//  }
}
