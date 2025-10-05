<!--
Sync Impact Report
==================
Version Change: 0.0.0 → 1.0.0
Bump Rationale: MINOR - Initial constitution adoption with comprehensive principles
Modified Principles: N/A (initial creation)
Added Sections: All core principles, Technology Constraints, Development Workflow, Governance
Removed Sections: None
Templates Status:
  ✅ .specify/templates/plan-template.md - Constitution Check section aligns with 5 principles
  ✅ .specify/templates/spec-template.md - No changes needed (tech-agnostic requirements)
  ✅ .specify/templates/tasks-template.md - Task categories include quality & performance
  ✅ .claude/commands/*.md - No agent-specific references to update
Follow-up TODOs: None
-->

# ETL Spark Gradle Constitution

## Core Principles

### I. Code Quality & Standards

All code MUST adhere to consistent quality standards to ensure maintainability and
collaboration effectiveness:

- Code MUST follow language-specific style guides (Google Java Style or Scala Style Guide)
- Static analysis tools (SpotBugs, ScalaStyle) MUST run in CI pipeline with zero violations
- All public APIs and complex transformations MUST include comprehensive documentation
- Code reviews MUST be completed by at least one team member before merge
- Cyclomatic complexity MUST stay below 10 per method
- No code duplication exceeding 5 consecutive lines

**Rationale**: ETL pipelines require long-term maintenance. Poor code quality leads to
brittle transformations, difficult debugging, and costly production incidents.

### II. Test-First Development (NON-NEGOTIABLE)

Test-Driven Development is mandatory for all feature work:

- Tests MUST be written before implementation code
- User approval of test scenarios MUST occur before implementation begins
- Tests MUST fail initially (Red), then pass after implementation (Green), then be
  refactored for clarity (Refactor)
- Unit test coverage MUST exceed 80% for transformation logic
- Integration tests MUST validate complete data pipelines end-to-end
- Contract tests MUST verify data schema expectations at system boundaries
- No implementation code merged without corresponding passing tests

**Rationale**: Data transformations are complex and error-prone. TDD ensures correctness,
prevents regressions, and serves as living documentation of expected behavior.

### III. Data Quality Assurance

Data integrity is paramount in ETL systems:

- Schema validation MUST occur at all ingestion points before processing
- Data quality checks (completeness, accuracy, consistency) MUST run before and after
  transformations
- Invalid or malformed data MUST be quarantined to error tables with detailed diagnostics
- Data lineage MUST be tracked and documented (source → transformation → destination)
- Transformations MUST be idempotent: same input produces identical output regardless
  of execution count
- Data quality metrics (null rates, duplicate rates, schema violations) MUST be published
  to monitoring systems
- Critical data quality failures MUST halt pipeline execution and alert operators

**Rationale**: Poor data quality cascades through downstream systems, eroding trust and
causing business impact. Prevention and early detection are essential.

### IV. Big Data Performance Requirements

Performance and scalability MUST be designed in, not bolted on:

- Every Spark job MUST explicitly declare partitioning strategy with documented rationale
- Data skew analysis MUST be performed on key columns before production deployment
- Performance benchmarks MUST be established for each pipeline (throughput, latency, cost)
- Resource allocations (executor memory, cores, instances) MUST be documented per job
- Spark query execution plans MUST be reviewed for common anti-patterns (shuffles,
  broadcasts, Cartesian products)
- Performance regression tests MUST run against representative data volumes in CI
- Job runtime exceeding 2x baseline MUST trigger alerts
- Caching strategies MUST be explicit and justified (memory cost vs. recomputation cost)

**Rationale**: Spark jobs process millions to billions of records. Inefficient code leads
to exponential costs, missed SLAs, and cluster resource exhaustion.

### V. Observability & Monitoring

ETL systems MUST be fully observable for operational excellence:

- Structured logging MUST include correlation IDs linking records across pipeline stages
- All log entries MUST include: timestamp, job ID, stage name, severity, context
- Metrics MUST track: records ingested, records processed, records failed, job duration,
  data quality scores, resource utilization
- Distributed tracing MUST capture cross-service interactions for complex workflows
- Alerts MUST be configured for: pipeline failures, SLA breaches, data quality threshold
  violations, resource saturation
- Job execution metadata (start time, end time, rows processed, status) MUST be persisted
  to audit tables for compliance and debugging
- Dashboards MUST provide real-time visibility into pipeline health and data flow

**Rationale**: Production data pipelines fail in unpredictable ways. Comprehensive
observability enables rapid diagnosis, reduces MTTR, and supports continuous improvement.

## Technology Constraints

To maintain consistency and leverage team expertise, the following technology standards
apply:

- **Language**: Java 11+ or Scala 2.12+ (compatible with Spark 3.x)
- **Build Tool**: Gradle 7+ with dependency lock files
- **Data Processing**: Apache Spark 3.x (structured APIs preferred: Dataset/DataFrame)
- **Testing Frameworks**: JUnit 5 (Java), ScalaTest (Scala), Spark Testing Base
- **Data Formats**: Parquet (preferred for storage), Avro (for schemas), JSON (APIs only)
- **Version Control**: Git with conventional commit messages
- **CI/CD**: GitHub Actions or equivalent with automated quality gates

Deviations require architecture review and documented justification.

## Development Workflow

All development MUST follow this process:

- Feature branches created from main with descriptive names (e.g., `feat/customer-dedup`)
- All commits MUST include tests demonstrating the change
- Pre-commit hooks enforce linting and formatting standards
- Pull requests MUST include: description, test evidence, performance impact assessment
- CI pipeline MUST pass (build, test, quality gates) before merge approval
- Code reviews MUST verify: test coverage, adherence to principles, performance
  considerations
- No direct commits to main branch; all changes via reviewed pull requests
- Merge commits use squash strategy to maintain clean history

## Governance

**Authority**: This constitution supersedes all other development practices, coding
standards, and team conventions. In case of conflict, constitutional principles prevail.

**Amendment Process**: Amendments to this constitution require:
1. Written proposal documenting rationale and impact analysis
2. Team review and approval (majority consensus)
3. Migration plan for existing code if breaking changes introduced
4. Version increment following semantic versioning rules

**Compliance Verification**: All pull requests MUST be reviewed for constitutional
compliance. Reviewers MUST explicitly confirm adherence to core principles or document
justified exceptions.

**Complexity Justification**: Any violation of constitutional principles MUST be justified
in writing with: (a) specific need, (b) simpler alternatives considered and rejected,
(c) mitigation plan for risks introduced.

**Version**: 1.0.0 | **Ratified**: 2025-10-05 | **Last Amended**: 2025-10-05
