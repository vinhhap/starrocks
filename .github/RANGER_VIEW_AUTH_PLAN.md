# Ranger view authorization fix plan

## Context
- Symptom: A user without SELECT on a view in a different database (default catalog) can still query it when Apache Ranger is the access controller.
- Current branch: fix/ranger-view-3.5.9. Target fix should be scoped and backportable.

## Root cause (from code reading)
- ColumnPrivilege routes external controllers (e.g., Ranger) into a column-only check path. View/table privilege checks are skipped for external controllers, so Ranger never enforces SELECT on the view itself.
- When a query hits a view, the optimizer’s logical scans are on underlying base tables. ScanColumnCollector maps table objects to names gathered from the AST (the view), so the base tables are missing, producing an empty column set for the view. With no columns and no view check, authorization is bypassed.
- Relevant code: ColumnPrivilege external branch and collector ([fe/fe-core/src/main/java/com/starrocks/authorization/ColumnPrivilege.java](fe/fe-core/src/main/java/com/starrocks/authorization/ColumnPrivilege.java)), Ranger access controller ([fe/fe-core/src/main/java/com/starrocks/authorization/ranger/starrocks/RangerStarRocksAccessController.java](fe/fe-core/src/main/java/com/starrocks/authorization/ranger/starrocks/RangerStarRocksAccessController.java)), Authorizer view/table entrypoints ([fe/fe-core/src/main/java/com/starrocks/sql/analyzer/Authorizer.java](fe/fe-core/src/main/java/com/starrocks/sql/analyzer/Authorizer.java)).

## Fix strategy
- Always enforce object-level SELECT on views/materialized views even when the catalog uses an ExternalAccessController (Ranger). Do not rely solely on column pruning in that path.
- Keep column-level checks for external controllers for base tables; for views, object-level checks are sufficient because Ranger supports view resources.
- Ensure TableName used for authorization is fully qualified (defaults to internal catalog if null) to match RangerStarRocksAccessController expectations.
- (Optional hardening) If time permits, guard ScanColumnCollector against null lookups to avoid silently grouping columns under null when base tables are not in the map; log/skip with context.

## Plan
1) Adjust ColumnPrivilege external-controller branch to invoke the appropriate object-level check:
   - Views -> Authorizer.checkViewAction
   - Materialized views -> Authorizer.checkMaterializedViewAction
   - Tables/others -> Authorizer.checkTableAction
   Continue to run column checks for tables where columns are collected.
2) Normalize TableName for external checks to default catalog when missing, to match Ranger controller expectations.
3) Add a defensive fix in ScanColumnCollector so missing table-to-name mappings are detected (and skipped) instead of inserting under a null key; consider augmenting the map with base tables if readily available.
4) Add regression tests:
   - Authorization test where Ranger is active and a user lacks SELECT on a view in another db: query fails.
   - Confirm base table column checks still enforced for external controllers.
5) Verify build/tests for fe-core auth suite; run targeted unit/integration tests if available (e.g., AuthorizationTest/AccessControlTest). Provide manual SQL repro steps in the PR description.

## TODO
- [x] Implement object-level checks for views/MVs/tables in the external-controller path of ColumnPrivilege.
- [x] Normalize TableName catalog for external checks when null (default internal catalog).
- [x] Harden ScanColumnCollector against missing table-name mappings (avoid null-key column buckets; optional map enrichment for base tables).
- [x] Add regression test for external-controller view SELECT denial across databases (ColumnPrivilegeExternalTest).
- [x] Add regression test to confirm base table column checks remain enforced for external controllers.
- [x] Run targeted fe-core test ColumnPrivilegeExternalTest in Docker (pass); broader auth suite still optional.
- [ ] Prepare PR notes: root cause, fix summary, test results, and backport considerations.
