package io.kestra.core.runners;

import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.scheduler.model.TriggerState;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.queues.QueueException;
import jakarta.validation.ConstraintViolationException;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

public interface SchedulerTriggerStateInterface {
    Optional<TriggerState> findLast(TriggerContext trigger);

    List<TriggerState> findAllForAllTenants();

    TriggerState save(TriggerState trigger, ScheduleContextInterface scheduleContext) throws ConstraintViolationException;

    TriggerState create(TriggerState trigger) throws ConstraintViolationException;

    TriggerState save(TriggerState trigger, ScheduleContextInterface scheduleContext, String headerContent) throws ConstraintViolationException;

    TriggerState create(TriggerState trigger, String headerContent) throws ConstraintViolationException;

    TriggerState update(TriggerState trigger);

    TriggerState update(Flow flow, AbstractTrigger abstractTrigger, ConditionContext conditionContext) throws Exception;

    /**
     * QueueException required for Kafka implementation
     */
    void delete(TriggerState trigger) throws QueueException;
    /**
     * Used by the JDBC implementation: find triggers in all tenants.
     */
    List<TriggerState> findByNextExecutionDateReadyForAllTenants(ZonedDateTime now, ScheduleContextInterface scheduleContext);

    /**
     * Used by the JDBC implementation: find ready but locked triggers
     */
    List<TriggerState> findByNextExecutionDateReadyButLockedTriggers(ZonedDateTime now);

    /**
     * Used by the Kafka implementation: find triggers in the scheduler assigned flow (as in Kafka partition assignment).
     */
    List<TriggerState> findByNextExecutionDateReadyForGivenFlows(List<FlowWithSource> flows, ZonedDateTime now, ScheduleContextInterface scheduleContext);
}
