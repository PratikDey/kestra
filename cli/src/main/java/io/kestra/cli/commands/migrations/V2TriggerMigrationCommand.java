package io.kestra.cli.commands.migrations;

import com.github.javaparser.utils.Log;
import io.kestra.cli.AbstractCommand;
import io.kestra.core.models.triggers.Trigger;
import io.kestra.core.models.triggers.TriggerId;
import io.kestra.core.repositories.TriggerRepositoryInterface;
import io.kestra.scheduler.SchedulerConfiguration;
import io.kestra.scheduler.model.TriggerState;
import io.kestra.scheduler.vnodes.VNodes;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.List;

public class V2TriggerMigrationCommand extends AbstractCommand {
    
    @Inject
    private ApplicationContext applicationContext;
    
    @CommandLine.Option(names = "--dry-run", description = "Preview only, do not update")
    boolean dryRun;
    
    @Override
    public Integer call() throws Exception {
        super.call();
        
        if (dryRun) {
            System.out.println("🧪 Dry-run mode enabled. No changes will be applied.");
        }
        
        Log.info("🔁 Starting trigger states migration...");
        TriggerRepositoryInterface repository = applicationContext.getBean(TriggerRepositoryInterface.class);
        SchedulerConfiguration configuration = applicationContext.getBean(SchedulerConfiguration.class);
        List<Trigger> triggers = repository.findAllForAllTenantsV1();
        Log.info("Found [{}] triggers to migrate.");
        triggers.forEach(trigger -> {
            try {
                TriggerState migrated = convert(trigger, configuration);
                if (!dryRun) {
                    repository.save(migrated);
                }
                System.out.println("✅ Migration complete for: " + TriggerId.of(trigger));
            } catch (Exception e) {
                System.err.println("❌ Migration failed for : " + TriggerId.of(trigger));
                e.printStackTrace();
            }
        });
        System.out.println("✅ Migration complete.");
        return 0;
    }
    
    public static TriggerState convert(Trigger trigger, SchedulerConfiguration configuration) {
        return TriggerState
            .builder()
            .tenantId(trigger.getTenantId())
            .namespace(trigger.getNamespace())
            .flowId(trigger.getFlowId())
            .triggerId(trigger.getTriggerId())
            .updatedAt(trigger.getUpdatedDate())
            .evaluatedAt(trigger.getDate())
            .nextEvaluationDate(trigger.getNextExecutionDate())
            .backfill(trigger.getBackfill())
            .stopAfter(trigger.getStopAfter())
            .disabled(trigger.getDisabled())
            .workerId(trigger.getWorkerId())
            .vnode(VNodes.computeVNodeFromTrigger(trigger, configuration.vnodes()))
            .locked(trigger.getExecutionId() != null)
            .build();
    }
}
