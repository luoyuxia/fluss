# Lake Table Promotion Client Reuse Plan

## 1. Background

`EnableFlussOnLakeTableProcedure` currently coordinates the complete promotion flow for a Paimon
log table:

1. Read the latest lake snapshot.
2. Create Fluss metadata on the existing lake table.
3. Register the initial lake snapshot for a log table.
4. Enable datalake on the Fluss table.

An external Fluss manager needs the same behavior. It must depend directly only on `fluss-client`,
without a compile-time dependency on `fluss-flink`. The manager will also support primary-key table
promotion by launching a Flink job, but its job submission and reconciliation mechanism differs
from the one used by the open-source Flink Procedure.

This plan extracts the common client-side promotion flow while keeping lake inspection and Flink
job lifecycle management in their current integration layers. It does not introduce a new Server
API or RPC.

## 2. Goals

- Share the log-table promotion flow between the Flink Procedure and external managers.
- Keep the shared implementation in `fluss-client`.
- Require no direct `fluss-flink` or `fluss-rpc` dependency from an external manager.
- Reuse one primary-key bulk-load job implementation while allowing different launch mechanisms.
- Preserve retry and snapshot-consistency behavior.
- Keep provider-specific status, persistence, credentials, and cleanup outside Fluss Client.

## 3. Module Boundaries

### 3.1 `fluss-client`

`fluss-client` owns the format-neutral promotion workflow:

- Create Fluss metadata from an existing lake table.
- Recover an existing Fluss table during a retry.
- Initialize or verify the lake snapshot for a log table.
- Enable datalake for a log table.
- Return a data-load requirement for a primary-key table.

`fluss-client` must not inspect Paimon directly and must not submit or monitor Flink jobs.

### 3.2 `fluss-flink`

`fluss-flink` owns the open-source integration:

- Resolve the latest source snapshot through `LakeCatalog`.
- Adapt the Flink Procedure arguments to the client promotion API.
- Launch and wait for the primary-key bulk-load job.
- Provide the shared Flink job implementation used to load lake data into Fluss.

### 3.3 External Manager

The manager owns its control-plane behavior:

- Resolve the latest source snapshot through its lake catalog integration.
- Persist promotion runs and reconcile their status.
- Submit the shared bulk-load job through its cloud-specific Flink deployment mechanism.
- Publish provider-specific status and sanitized errors.
- Apply its own failure cleanup policy.

The manager calls the common promotion API through `fluss-client` and does not compile against
`fluss-flink`. It references the shared Flink job through its artifact URI, entry class, and command
arguments.

## 4. Proposed Client API

Add a format-neutral promoter to `fluss-client`. The exact names can be adjusted during
implementation, but the contract should be equivalent to the following:

```java
public final class LakeTablePromoter {

    public LakeTablePromoter(Admin admin, Configuration configuration);

    public CompletableFuture<LakeTablePromotionResult> promote(
            TablePath tablePath,
            Map<String, String> options,
            @Nullable Long sourceSnapshotId);
}
```

The result describes whether promotion completed in the client or requires a bulk-load job:

```java
public final class LakeTablePromotionResult {

    public enum Status {
        ENABLED,
        DATA_LOAD_REQUIRED
    }

    public Status getStatus();

    public TableInfo getTableInfo();

    @Nullable
    public Long getSourceSnapshotId();
}
```

The source snapshot is supplied by the caller because discovering it requires a lake-format
catalog or a provider-specific catalog service. The promoter does not accept a Flink job launcher.

## 5. Promotion Flow

### 5.1 Common Preparation

`LakeTablePromoter.promote()` performs the following steps:

1. Call `Admin.createTableOnLake(tablePath, options)`.
2. If the table already exists, call `Admin.getTableInfo(tablePath)` and continue as an idempotent
   retry.
3. If datalake is already enabled, return `ENABLED`.
4. Select the remaining flow from `TableInfo.hasPrimaryKey()`.

The caller does not derive the table type independently. The Fluss table metadata returned by the
Server is authoritative.

### 5.2 Log Table

For a log table, the promoter prepares the initial lake snapshot before enabling datalake:

1. If the source lake table has no snapshot, skip snapshot registration.
2. If Fluss has no registered lake snapshot, commit the source snapshot with empty bucket offsets
   and empty max-tiered timestamps.
3. If Fluss already registered the same snapshot, treat the request as a retry.
4. If the registered snapshot differs from the supplied source snapshot, reject the promotion.
5. Set `table.datalake.enabled=true` through the existing `Admin.alterTable()` API.
6. Return `ENABLED`.

The existing Server-side alter-table validation remains the final schema and snapshot consistency
check.

### 5.3 Primary-Key Table

For a primary-key table, the promoter keeps datalake disabled and returns
`DATA_LOAD_REQUIRED`. The result contains the created or recovered `TableInfo` and the source
snapshot ID.

The caller then launches the shared Flink bulk-load job. This rule also applies to an empty
primary-key table; the job may finish quickly, but it remains responsible for the primary-key
promotion lifecycle and final enable operation.

## 6. Snapshot Committer Placement

Move `FlussTableLakeSnapshotCommitter` from `fluss-flink-common` to an internal package in
`fluss-client`.

The class currently uses Fluss configuration, metadata, metrics, and RPC classes without using a
Flink API. Moving it provides one implementation of the prepare/commit protocol for:

- Flink tiering.
- `LakeTablePromoter` initial snapshot registration.
- External managers through the public promotion API.

The committer remains an implementation detail. External managers use `LakeTablePromoter` and do
not call Fluss RPC classes directly. A manager can therefore remove its copied snapshot committer
and its direct Maven dependency on `fluss-rpc`.

## 7. Shared Primary-Key Bulk-Load Job

Implement the primary-key load operation once in the Flink action artifact, for example through:

```text
FlussActionEntrypoint load_lake_data_to_fluss
```

The job receives a stable, format-neutral set of arguments:

- Fluss bootstrap and security configuration.
- Lake catalog configuration.
- Table path and Fluss table ID.
- Source snapshot ID.
- A deterministic bulk-load caller token.
- Bulk-load storage configuration.

The job performs:

1. Read the specified lake snapshot.
2. Load its records into Fluss through the Bulk Load API.
3. Commit the Bulk Load operation.
4. Set `table.datalake.enabled=true` with the existing `Admin.alterTable()` API.

The final alter operation reuses the current Server-side validation. If the source snapshot or
table definition changed, enable fails and the table remains lake-disabled.

The open-source Procedure and the manager run the same artifact but use different launch paths:

```text
EnableFlussOnLakeTableProcedure
  -> open-source Flink launcher
  -> shared load_lake_data_to_fluss job

External manager
  -> provider-specific deployment/reconciliation
  -> shared load_lake_data_to_fluss job
```

No job-launcher interface is added to `fluss-client`. The Procedure is synchronous while a manager
normally persists and reconciles remote jobs asynchronously; their lifecycle contracts should
remain separate.

## 8. Procedure Changes

After extracting the client workflow, `EnableFlussOnLakeTableProcedure` is reduced to:

1. Parse the table identifier and options.
2. Create the lake catalog and read the latest source snapshot.
3. Call `LakeTablePromoter.promote()`.
4. Return success for `ENABLED`.
5. For `DATA_LOAD_REQUIRED`, launch the shared Flink job and wait for completion.
6. Verify that the Fluss table is datalake-enabled before returning success.

The Procedure no longer creates Fluss metadata, registers the initial snapshot, or alters the log
table directly.

## 9. Manager Changes

The manager-side client adapter calls `LakeTablePromoter.promote()` after its existing lake-table
inspection.

For `ENABLED`, the manager publishes its succeeded status immediately. For
`DATA_LOAD_REQUIRED`, it persists a run and uses its existing scheduler and provider-specific
plugin to create, start, and observe the shared job.

The manager can remove:

- Its local `FlussTableLakeSnapshotCommitter` implementation.
- Its duplicate log-table create/register/enable orchestration.
- Its direct `fluss-rpc` dependency.

The following behavior remains manager-specific:

- Promotion-run persistence and deterministic cloud deployment identity.
- Remote job reconciliation.
- Provider-specific lake-table status options.
- Credential and artifact injection.
- Cleanup after a failed bulk load.

When the bulk-load job reports success, the manager verifies that the Fluss table is already
datalake-enabled. The job performs the enable operation.

## 10. Retry and Failure Semantics

### Log Table

- Repeating a completed promotion returns `ENABLED`.
- Repeating after metadata creation but before snapshot registration resumes registration.
- Repeating after snapshot registration accepts the same source snapshot.
- A different source snapshot is rejected while the table remains lake-disabled.

### Primary-Key Table

- Repeating preparation returns `DATA_LOAD_REQUIRED` with the same table metadata and source
  snapshot while the source has not changed.
- The caller uses a deterministic bulk-load token to avoid creating multiple logical loads.
- The job enables datalake only after Bulk Load commits successfully.
- A failed job leaves datalake disabled and can be retried or cleaned up by the caller.

## 11. Delivery Plan

### PR 1: Extract the Client Promotion Workflow

- Move `FlussTableLakeSnapshotCommitter` to `fluss-client`.
- Add `LakeTablePromoter` and `LakeTablePromotionResult`.
- Support the existing log-table promotion flow.
- Change `EnableFlussOnLakeTableProcedure` to use the promoter.
- Preserve current Procedure behavior and SQL signatures.

### PR 2: Add Primary-Key Promotion

- Return `DATA_LOAD_REQUIRED` for primary-key tables.
- Add the shared `load_lake_data_to_fluss` job.
- Add the open-source Flink launcher used by the Procedure.
- Enable datalake from the job after a successful Bulk Load.

### Manager Integration

- Replace the duplicated manager flow with `LakeTablePromoter`.
- Map `DATA_LOAD_REQUIRED` to the existing persisted scheduler workflow.
- Submit the shared Flink job through the manager's provider-specific launcher.
- Remove the copied committer and direct `fluss-rpc` dependency.

## 12. Acceptance Criteria

- The log-table Procedure retains its current behavior and retry semantics.
- The manager can promote a log table with only a direct `fluss-client` dependency.
- The manager contains no copied Fluss lake-snapshot RPC implementation.
- Both launch paths execute the same primary-key bulk-load job implementation.
- A primary-key table is enabled only after Bulk Load succeeds.
- Snapshot mismatch leaves the table lake-disabled and produces a clear failure.
- Retrying a completed promotion does not submit another logical bulk-load operation.
