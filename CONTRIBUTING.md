# Contributing to ETL Spark Gradle

Thank you for your interest in contributing to the ETL Spark Gradle project! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Submitting Changes](#submitting-changes)
- [Code Review Process](#code-review-process)

## Code of Conduct

### Our Pledge

We are committed to providing a welcoming and inclusive environment for all contributors. We expect all participants to:

- Be respectful and professional
- Welcome diverse perspectives
- Focus on constructive feedback
- Prioritize the project's best interests

## Getting Started

### Prerequisites

- **Java**: JDK 11 or later
- **Scala**: 2.12.18
- **Gradle**: 7.6.5 (use included wrapper)
- **Git**: Latest version
- **IDE**: IntelliJ IDEA (recommended) or VS Code with Scala Metals

### Setting Up Your Development Environment

1. **Fork the repository**:
   ```bash
   # Click "Fork" on GitHub, then clone your fork
   git clone https://github.com/YOUR_USERNAME/etl-spark-gradle.git
   cd etl-spark-gradle
   ```

2. **Add upstream remote**:
   ```bash
   git remote add upstream https://github.com/ORIGINAL_OWNER/etl-spark-gradle.git
   ```

3. **Install dependencies**:
   ```bash
   ./gradlew build
   ```

4. **Start development services**:
   ```bash
   docker-compose up -d
   ```

5. **Verify setup**:
   ```bash
   ./gradlew test
   ```

### Project Structure Familiarization

Review these documents before contributing:
- [README.md](README.md) - Project overview
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) - Architecture and design patterns
- [docs/CONFIGURATION.md](docs/CONFIGURATION.md) - Configuration reference

## Development Workflow

### 1. Create a Feature Branch

Always create a new branch for your work:

```bash
# Update your main branch
git checkout main
git pull upstream main

# Create feature branch
git checkout -b feature/your-feature-name
# or
git checkout -b fix/issue-number-description
```

**Branch Naming Conventions**:
- `feature/` - New features
- `fix/` - Bug fixes
- `refactor/` - Code refactoring
- `docs/` - Documentation updates
- `test/` - Test improvements

### 2. Make Your Changes

Follow the [Coding Standards](#coding-standards) section below.

### 3. Test Your Changes

```bash
# Run all tests
./gradlew test

# Run specific test
./gradlew test --tests "com.etl.extractor.KafkaExtractorSpec"

# Check test coverage
./gradlew test jacocoTestReport
open build/reports/jacoco/test/html/index.html
```

### 4. Commit Your Changes

Follow conventional commit format:

```bash
git add .
git commit -m "feat: add Redis extractor implementation

- Implement DataExtractor trait for Redis
- Add unit tests with 85% coverage
- Update documentation

Closes #123"
```

**Commit Message Format**:
```
<type>: <subject>

<body>

<footer>
```

**Types**:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `test`: Test additions/modifications
- `refactor`: Code refactoring
- `perf`: Performance improvements
- `chore`: Build/tooling changes

### 5. Keep Your Branch Updated

```bash
git fetch upstream
git rebase upstream/main
```

### 6. Push Your Changes

```bash
git push origin feature/your-feature-name
```

## Coding Standards

### Scala Style Guide

We follow the [Scala Style Guide](https://docs.scala-lang.org/style/) with these additions:

#### Naming Conventions

```scala
// Classes and traits: PascalCase
class KafkaExtractor extends DataExtractor
trait DataTransformer

// Methods and variables: camelCase
def extractData(config: SourceConfig): DataFrame
val recordCount = df.count()

// Constants: PascalCase
val DefaultPartitionCount = 200

// Type parameters: Single uppercase letter
class Registry[T <: Component]
```

#### Code Organization

```scala
// Order within a file:
// 1. Package declaration
// 2. Imports (grouped: Scala, Java, Third-party, Project)
// 3. Class/Object definition
// 4. Companion object (if any)

package com.etl.extractor

import scala.collection.mutable
import java.nio.file.Path
import org.apache.spark.sql.DataFrame
import com.etl.core.{DataExtractor, SourceConfig}

class KafkaExtractor extends DataExtractor {
  // Public methods first
  override def extract(config: SourceConfig): DataFrame = ???

  // Private methods last
  private def parseOptions(options: Map[String, String]): KafkaConfig = ???
}
```

#### Documentation

Every public class and method must have ScalaDoc:

```scala
/**
 * Extracts data from Kafka topics.
 *
 * This extractor supports:
 * - Multiple topics via pattern subscription
 * - Configurable starting offsets
 * - SSL/SASL authentication
 *
 * @param vaultClient Vault client for retrieving credentials
 * @param lineageTracker Lineage metadata tracker
 */
class KafkaExtractor(
  vaultClient: VaultClient,
  lineageTracker: LineageTracker
) extends DataExtractor {

  /**
   * Extracts data from Kafka topic.
   *
   * @param config Source configuration including Kafka options
   * @param runContext Execution context with pipeline metadata
   * @return Extraction result with DataFrame and metrics
   * @throws ExtractionException if Kafka connection fails
   */
  override def extract(
    config: SourceConfig,
    runContext: RunContext
  ): ExtractionResult = {
    // Implementation
  }
}
```

#### Formatting

- **Indentation**: 2 spaces (no tabs)
- **Line length**: Maximum 120 characters
- **Blank lines**: One blank line between methods
- **Braces**: K&R style (opening brace on same line)

```scala
// Good
def transform(input: DataFrame): DataFrame = {
  input
    .filter($"age" > 18)
    .groupBy($"category")
    .count()
}

// Bad
def transform(input: DataFrame): DataFrame =
{
    input.filter($"age" > 18).groupBy($"category").count()
}
```

### Code Quality Tools

Run these before committing:

```bash
# ScalaStyle check
./gradlew scalaStyle

# Compiler warnings as errors
./gradlew compileScala -Dscala.compiler.fatal.warnings=true
```

## Testing Guidelines

### Test-Driven Development (TDD)

**MANDATORY**: All new features must follow TDD:

1. **Red**: Write a failing test
2. **Green**: Write minimal code to pass the test
3. **Refactor**: Improve code while keeping tests passing

### Test Structure

```scala
class KafkaExtractorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // Setup
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Test")
      .master("local[2]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  // Test cases
  "KafkaExtractor" should "extract data from topic" in {
    // Arrange
    val config = SourceConfig(...)
    val extractor = new KafkaExtractor()

    // Act
    val result = extractor.extract(config, runContext)

    // Assert
    result.isSuccess shouldBe true
    result.recordsExtracted should be > 0L
  }

  it should "handle connection failure gracefully" in {
    // Test error handling
  }
}
```

### Test Coverage Requirements

- **Overall coverage**: Minimum 80%
- **New features**: Minimum 85%
- **Critical paths**: 100% (extraction, loading, data quality)

### Test Types

1. **Unit Tests**: Test individual components in isolation
   - Location: `src/test/scala/com/etl/`
   - Pattern: `*Spec.scala`

2. **Contract Tests**: Verify interface implementations
   - Test all implementations adhere to contracts
   - Example: `DataExtractorContractSpec.scala`

3. **Integration Tests**: Test end-to-end scenarios
   - Location: `src/test/scala/com/etl/integration/`
   - Use embedded databases, local filesystem

4. **Performance Tests**: Benchmark performance
   - Location: `src/test/scala/com/etl/benchmark/`
   - Run separately from CI (long-running)

### Mocking Guidelines

Use mocks for external dependencies:

```scala
// Mock Vault client
val mockVault = VaultClient.mock()
mockVault.writeSecret("path", Map("key" -> "value"))

// Use H2 for database testing
val jdbcUrl = "jdbc:h2:mem:testdb;MODE=PostgreSQL"
```

## Submitting Changes

### Pull Request Process

1. **Update documentation**:
   - Update README if behavior changes
   - Add/update ScalaDoc comments
   - Update CHANGELOG.md

2. **Ensure tests pass**:
   ```bash
   ./gradlew test
   ./gradlew jacocoTestCoverageVerification
   ```

3. **Create pull request**:
   - Use descriptive title
   - Fill out PR template completely
   - Reference related issues
   - Add screenshots/examples if applicable

4. **PR Title Format**:
   ```
   feat: Add Redis extractor
   fix: Correct null handling in PostgresLoader
   docs: Update configuration examples
   ```

### Pull Request Template

When creating a PR, include:

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] All tests passing
- [ ] Coverage threshold met (80%+)

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-reviewed code
- [ ] Commented complex logic
- [ ] Updated documentation
- [ ] No new warnings
- [ ] Added tests proving fix/feature works
- [ ] Dependent changes merged

## Related Issues
Closes #123
Relates to #456
```

## Code Review Process

### Review Criteria

Reviewers will check:

1. **Correctness**: Code works as intended
2. **Tests**: Adequate test coverage
3. **Style**: Follows coding standards
4. **Performance**: No obvious inefficiencies
5. **Documentation**: Clear and complete
6. **Security**: No vulnerabilities introduced

### Addressing Review Feedback

1. Make requested changes
2. Commit with descriptive messages
3. Push to your branch
4. Reply to review comments
5. Request re-review when ready

### Approval Requirements

- At least 1 approval from maintainer
- All CI checks passing
- No unresolved conversations

## Additional Guidelines

### Adding New Components

When adding extractors/transformers/loaders:

1. **Implement trait**:
   ```scala
   class RedisExtractor extends DataExtractor {
     override def sourceType: String = "redis"
     override def extract(...): ExtractionResult = ???
     override def validateConfig(...): Unit = ???
   }
   ```

2. **Write contract tests**:
   ```scala
   class RedisExtractorSpec extends DataExtractorContractSpec {
     override def createExtractor(): DataExtractor = new RedisExtractor()
   }
   ```

3. **Write unit tests**:
   - Test happy path
   - Test error conditions
   - Test edge cases
   - Achieve 85%+ coverage

4. **Register component**:
   ```scala
   // In appropriate registry
   extractorRegistry.register("redis", new RedisExtractor())
   ```

5. **Update documentation**:
   - Add to README.md
   - Add configuration example to CONFIGURATION.md
   - Create example pipeline YAML

### Performance Considerations

- Profile code with large datasets
- Avoid unnecessary shuffles
- Use appropriate partitioning
- Cache judiciously
- Monitor memory usage

### Security Best Practices

- Never commit secrets
- Use Vault for credentials
- Validate all inputs
- Sanitize user-provided SQL
- Keep dependencies updated

## Getting Help

- **Questions**: Open a GitHub Discussion
- **Bugs**: Create an issue with bug template
- **Features**: Create an issue with feature template
- **Chat**: Join our Slack channel (if available)

## Recognition

Contributors will be:
- Listed in CONTRIBUTORS.md
- Mentioned in release notes
- Credited in commit history

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.

---

Thank you for contributing to ETL Spark Gradle! 🎉
