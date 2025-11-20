package io.kestra.jdbc.repository;

import io.kestra.core.exceptions.DeserializationException;
import io.kestra.core.models.QueryFilter;
import io.kestra.core.models.QueryFilter.Resource;
import io.kestra.core.models.dashboards.ColumnDescriptor;
import io.kestra.core.models.dashboards.DataFilter;
import io.kestra.core.models.dashboards.DataFilterKPI;
import io.kestra.core.models.dashboards.filters.AbstractFilter;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.Trigger;
import io.kestra.core.models.triggers.TriggerId;
import io.kestra.core.repositories.ArrayListTotal;
import io.kestra.core.repositories.TriggerRepositoryInterface;
import io.kestra.core.runners.QueueIndexerRepository;
import io.kestra.core.runners.TransactionContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.DateUtils;
import io.kestra.core.utils.ListUtils;
import io.kestra.jdbc.JdbcMapper;
import io.kestra.jdbc.runner.JdbcTransactionContext;
import io.kestra.jdbc.services.JdbcFilterService;
import io.kestra.plugin.core.dashboard.data.ITriggers;
import io.kestra.plugin.core.dashboard.data.Triggers;
import io.kestra.scheduler.model.TriggerState;
import io.micronaut.data.model.Pageable;
import jakarta.annotation.Nullable;
import lombok.Getter;
import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.DSL;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractJdbcTriggerRepository extends AbstractJdbcRepository implements TriggerRepositoryInterface, QueueIndexerRepository<TriggerState> {
    public static final Field<Object> NAMESPACE_FIELD = field("namespace");
    private static final Field<Long> NEXT_EVALUATION_EPOCH_FIELD = field("next_evaluation_epoch", Long.class);
    private static final Field<Boolean> LOCKED_FIELD = field("locked", Boolean.class);
    private static final Field<Integer> VNODE_FIELD = field("vnode", Integer.class);
    
    protected io.kestra.jdbc.AbstractJdbcRepository<TriggerState> jdbcRepository;

    private final JdbcFilterService filterService;

    @Getter
    private final Map<Triggers.Fields, String> fieldsMapping = Map.of(
        Triggers.Fields.ID, "key",
        Triggers.Fields.NAMESPACE, NAMESPACE_FIELD.getName(),
        Triggers.Fields.FLOW_ID, "flow_id",
        Triggers.Fields.TRIGGER_ID, "trigger_id",
        Triggers.Fields.EXECUTION_ID, "execution_id",
        Triggers.Fields.NEXT_EXECUTION_DATE, NEXT_EVALUATION_EPOCH_FIELD.getName(),
        Triggers.Fields.WORKER_ID, "worker_id"
    );

    @Override
    public Set<Triggers.Fields> dateFields() {
        return Set.of();
    }

    @Override
    public Triggers.Fields dateFilterField() {
        return null;
    }

    public AbstractJdbcTriggerRepository(io.kestra.jdbc.AbstractJdbcRepository<TriggerState> jdbcRepository,
                                         JdbcFilterService filterService) {
        this.jdbcRepository = jdbcRepository;
        this.filterService = filterService;
    }

    @Override
    public Optional<TriggerState> findById(TriggerId trigger) {
        return this.jdbcRepository
            .getDslContextWrapper()
            .transactionResult(configuration -> {
                SelectConditionStep<Record1<Object>> select = DSL
                    .using(configuration)
                    .select(field("value"))
                    .from(this.jdbcRepository.getTable())
                    .where(field("key").eq(trigger.uid()));

                return this.jdbcRepository.fetchOne(select);
            });
    }

    @Override
    public Optional<TriggerState> findByExecution(Execution execution) {
        return this.jdbcRepository
            .getDslContextWrapper()
            .transactionResult(configuration -> {
                SelectConditionStep<Record1<Object>> select = DSL
                    .using(configuration)
                    .select(field("value"))
                    .from(this.jdbcRepository.getTable())
                    .where(
                        field("execution_id").eq(execution.getId())
                    );

                return this.jdbcRepository.fetchOne(select);
            });
    }

    @Override
    public List<TriggerState> findAll(String tenantId) {
        return this.jdbcRepository
            .getDslContextWrapper()
            .transactionResult(configuration -> {
                var select = DSL
                    .using(configuration)
                    .select(field("value"))
                    .from(this.jdbcRepository.getTable())
                    .where(this.defaultFilter(tenantId));

                return this.jdbcRepository.fetch(select);
            });
    }

    @Override
    public List<TriggerState> findAllForAllTenants() {
        return this.jdbcRepository
            .getDslContextWrapper()
            .transactionResult(configuration -> {
                SelectJoinStep<Record1<Object>> select = DSL
                    .using(configuration)
                    .select(field("value"))
                    .from(this.jdbcRepository.getTable());

                return this.jdbcRepository.fetch(select);
            });
    }

    @Override
    public int count(@Nullable String tenantId) {
        return this.jdbcRepository
            .getDslContextWrapper()
            .transactionResult(configuration -> DSL
                .using(configuration)
                .selectCount()
                .from(this.jdbcRepository.getTable())
                .where(this.defaultFilter(tenantId))
                .fetchOne(0, int.class));
    }
    
    @Override
    public TriggerState save(TriggerState trigger) {
        Map<Field<Object>, Object> fields = this.jdbcRepository.persistFields(trigger);
        this.jdbcRepository.persist(trigger, fields);
        
        return trigger;
    }
    
    @Override
    public TriggerState save(TransactionContext txContext, TriggerState trigger) {
        return save(txContext.unwrap(JdbcTransactionContext.class).getDslContext(), trigger);
    }
    
    @Override
    public <TX extends TransactionContext> boolean supports(Class<TX> clazz) {
        return JdbcTransactionContext.class.isAssignableFrom(clazz);
    }
    
    private TriggerState save(DSLContext dslContext, TriggerState trigger) {
        Map<Field<Object>, Object> fields = this.jdbcRepository.persistFields(trigger);
        this.jdbcRepository.persist(trigger, dslContext, fields);
        
        return trigger;
    }

    @Override
    public Class<TriggerState> getItemClass() {
        return TriggerState.class;
    }

    public TriggerState create(TriggerState trigger) {
        return this.jdbcRepository
            .getDslContextWrapper()
            .transactionResult(configuration -> {
                DSL.using(configuration)
                    .insertInto(this.jdbcRepository.getTable())
                    .set(AbstractJdbcRepository.field("key"), this.jdbcRepository.key(trigger))
                    .set(this.jdbcRepository.persistFields(trigger))
                    .execute();

                return trigger;
            });
    }

    @Override
    public void delete(TriggerState trigger) {
        this.jdbcRepository.delete(trigger);
    }

    @Override
    public TriggerState update(TriggerState trigger) {
        return this.jdbcRepository
            .getDslContextWrapper()
            .transactionResult(configuration -> {
                DSL.using(configuration)
                    .update(this.jdbcRepository.getTable())
                    .set(this.jdbcRepository.persistFields((trigger)))
                    .where(field("key").eq(trigger.uid()))
                    .execute();

                return trigger;
            });
    }
    
    @Override
    public ArrayListTotal<TriggerState> find(Pageable pageable, String tenantId, List<QueryFilter> filters) {
        return this.jdbcRepository
            .getDslContextWrapper()
            .transactionResult(configuration -> {
                DSLContext context = DSL.using(configuration);
                SelectConditionStep<?> select = generateSelect(context, tenantId, filters);
                return this.jdbcRepository.fetchPage(context, select, pageable);
            });
    }

    private SelectConditionStep<?> generateSelect(DSLContext context, String tenantId, List<QueryFilter> filters){
        SelectConditionStep<?> select = context
            .select(field("value"))
            .from(this.jdbcRepository.getTable())
            .where(this.defaultFilter(tenantId));

        return select.and(filter(filters, NEXT_EVALUATION_EPOCH_FIELD.getName(), true, Resource.TRIGGER));
    }

    @Override
    public ArrayListTotal<TriggerState> find(Pageable pageable, String query, String tenantId, String namespace, String flowId, String workerId) {
        return this.jdbcRepository
            .getDslContextWrapper()
            .transactionResult(configuration -> {
                DSLContext context = DSL.using(configuration);

                SelectConditionStep<Record1<Object>> select = context
                    .select(field("value"))
                    .from(this.jdbcRepository.getTable())
                    .where(this.fullTextCondition(query))
                    .and(this.defaultFilter(tenantId));

                if (namespace != null) {
                    select.and(DSL.or(NAMESPACE_FIELD.eq(namespace), NAMESPACE_FIELD.likeIgnoreCase(namespace + ".%")));
                }

                if (flowId != null) {
                    select.and(field("flow_id").eq(flowId));
                }

                if (workerId != null) {
                    select.and(field("worker_id").eq(workerId));
                }
                select.and(this.defaultFilter());

                return this.jdbcRepository.fetchPage(context, select, pageable);
            });
    }

    /** {@inheritDoc} */
    @Override
    public Flux<TriggerState> find(String tenantId, List<QueryFilter> filters) {
        return Flux.create(
            emitter -> this.jdbcRepository
                .getDslContextWrapper()
                .transaction(configuration -> {
                    DSLContext context = DSL.using(configuration);
                    SelectConditionStep<?> select = generateSelect(context, tenantId, filters);

                    select.fetch()
                    .map(this.jdbcRepository::map)
                    .forEach(emitter::next);

                    emitter.complete();

                }),
            FluxSink.OverflowStrategy.BUFFER
        );

    }
    
    protected Condition fullTextCondition(String query) {
        return query == null ? DSL.trueCondition() : jdbcRepository.fullTextCondition(List.of("fulltext"), query);
    }
    
    @Override
    protected Condition findQueryCondition(String query) {
        return fullTextCondition(query);
    }
    
    @Override
    protected Condition defaultFilter(String tenantId, boolean allowDeleted) {
        return buildTenantCondition(tenantId);
    }

    @Override
    protected Condition defaultFilter() {
        return DSL.trueCondition();
    }

    @Override
    public Function<String, String> sortMapping() throws IllegalArgumentException {
        Map<String, String> mapper = Map.of(
            "flowId", "flow_id",
            "triggerId", "trigger_id",
            "executionId", "execution_id",
            "nextExecutionDate", NEXT_EVALUATION_EPOCH_FIELD.getName()
        );

        return s -> mapper.getOrDefault(s, s);
    }

    @Override
    public ArrayListTotal<Map<String, Object>> fetchData(
        String tenantId,
        DataFilter<Triggers.Fields, ? extends ColumnDescriptor<Triggers.Fields>> descriptors,
        ZonedDateTime startDate,
        ZonedDateTime endDate,
        Pageable pageable
    ) {
        return this.jdbcRepository
            .getDslContextWrapper()
            .transactionResult(configuration -> {
                DSLContext context = DSL.using(configuration);

                Map<String, ? extends ColumnDescriptor<Triggers.Fields>> columnsWithoutDate = descriptors.getColumns().entrySet().stream()
                    .filter(entry -> entry.getValue().getField() == null || !dateFields().contains(entry.getValue().getField()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                boolean hasAgg = descriptors.getColumns().entrySet().stream().anyMatch(col -> col.getValue().getAgg() != null);
                // Generate custom fields for date as they probably need formatting
                // If they don't have aggs, we format datetime to minutes
                List<Field<Date>> dateFields = generateDateFields(descriptors, fieldsMapping, startDate, endDate, dateFields(), hasAgg ? null : DateUtils.GroupType.MINUTE);

                // Init request
                SelectConditionStep<Record> selectConditionStep = select(
                    context,
                    filterService,
                    columnsWithoutDate,
                    dateFields,
                    this.getFieldsMapping(),
                    this.jdbcRepository.getTable(),
                    tenantId
                );

                // Apply Where filter
                selectConditionStep = where(selectConditionStep, filterService, descriptors.getWhere(), fieldsMapping);

                List<? extends ColumnDescriptor<Triggers.Fields>> columnsWithoutDateWithOutAggs = columnsWithoutDate.values().stream()
                    .filter(column -> column.getAgg() == null)
                    .toList();

                // Apply GroupBy for aggregation
                SelectHavingStep<Record> selectHavingStep = groupBy(
                    selectConditionStep,
                    columnsWithoutDateWithOutAggs,
                    dateFields,
                    fieldsMapping
                );

                // Apply OrderBy
                SelectSeekStepN<Record> selectSeekStep = orderBy(selectHavingStep, descriptors);

                // Fetch and paginate if provided
                return fetchSeekStep(selectSeekStep, pageable);
            });
    }
    
    @Override
    public Double fetchValue(String tenantId, DataFilterKPI<ITriggers.Fields, ? extends ColumnDescriptor<ITriggers.Fields>> dataFilter, ZonedDateTime startDate, ZonedDateTime endDate, boolean numeratorFilter) {
        return this.jdbcRepository.getDslContextWrapper().transactionResult(configuration -> {
            DSLContext context = DSL.using(configuration);
            ColumnDescriptor<ITriggers.Fields> columnDescriptor = dataFilter.getColumns();
            String columnKey = this.getFieldsMapping().get(columnDescriptor.getField());
            Field<?> field = columnToField(columnDescriptor, getFieldsMapping());
            if (columnDescriptor.getAgg() != null) {
                field = filterService.buildAggregation(field, columnDescriptor.getAgg());
            }

            List<AbstractFilter<ITriggers.Fields>> filters = new ArrayList<>(ListUtils.emptyOnNull(dataFilter.getWhere()));
            if (numeratorFilter) {
                filters.addAll(dataFilter.getNumerator());
            }

            SelectConditionStep selectStep = context
                .select(field)
                .from(this.jdbcRepository.getTable())
                .where(this.defaultFilter(tenantId));

            var selectConditionStep = where(
                selectStep,
                filterService,
                filters,
                getFieldsMapping()
            );

            Record result = selectConditionStep.fetchOne();
            if (result != null) {
                return result.getValue(field, Double.class);
            } else {
                return null;
            }
        });
    }
    
    /**
     * {@inheritDoc}
     **/
    @Override
    public List<TriggerState> findTriggersEligibleForScheduling(ZonedDateTime now, Set<Integer> vNodes, boolean locked) {
        final long epochMilli = now.toInstant().toEpochMilli();
        return this.jdbcRepository
            .getDslContextWrapper()
            .transactionResult(configuration -> DSL.using(configuration)
                .select(field("value"))
                .from(this.jdbcRepository.getTable())
                .where(NEXT_EVALUATION_EPOCH_FIELD.le(epochMilli).or(NEXT_EVALUATION_EPOCH_FIELD.isNull()))
                .and(LOCKED_FIELD.isNull().or(LOCKED_FIELD.eq(locked)))
                .and(VNODE_FIELD.in(vNodes))
                .orderBy(NEXT_EVALUATION_EPOCH_FIELD.asc())
                .fetch()
            )
            .map(r -> this.jdbcRepository.deserialize(r.get("value", String.class)));
    }
    
    @Override
    abstract protected Field<Date> formatDateField(String dateField, DateUtils.GroupType groupType);
    
    @Override
    public List<Trigger> findAllForAllTenantsV1() {
        return this.jdbcRepository
            .getDslContextWrapper()
            .transactionResult(configuration -> {
                SelectJoinStep<Record1<Object>> select = DSL
                    .using(configuration)
                    .select(field("value"))
                    .from(this.jdbcRepository.getTable());
                
                return select.fetch().map(record -> {
                    String json = record.get("value", String.class);
                    try {
                        return JdbcMapper.of().readValue(json, Trigger.class);
                    } catch (IOException e) {
                        throw new DeserializationException(e, json);
                    }
                });
            });
    }
}
