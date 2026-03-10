# Changelog

All notable changes to this project will be documented in this file.
This file is automatically updated by the release workflow.

## [v1.0.0](https://github.com/tiagobalabuch/fabric-warehouse-advisor/releases/tag/v1.0.0)

- Merge pull request #7 from tiagobalabuch/feature/performance-advisor
- Bump version to 1.0.0, update installation instructions, and enhance version loading with warnings for better user feedback
- Enhance query regression checks to use window functions for median calculations and improve error handling in statistics health checks
- Refactor performance check phases to improve logging and timing, and enhance SQL identifier handling in statistics checks
- Update performance check documentation and version in pyproject.toml
- Refactor performance check findings to introduce new severity levels and update related logic for caching, collation, data types, and query regression checks
- Refactor table naming logic to enforce Warehouse identifier limits and enhance documentation for warehouse query functions with parameter details and retry logic
- Enhance table naming in recommendations to respect Warehouse identifier limits and add automatic retry logic with exponential back-off for warehouse table reads
- Update troubleshooting documentation to use placeholder for version in installation instructions
- Improve error handling in warehouse query reading and enhance version loading fallback
- Add query regression detection to Performance Check Advisor
- Add core modules for Fabric Warehouse Advisor
- Merge pull request #6 from tiagobalabuch/changelog/v0.3.0
- Update CHANGELOG.md for v0.3.0
- Merge pull request #5 from tiagobalabuch/docs/readme-install-steps
- Improve documentation clarity in installation and how it works guides
- Update documentation for clarity and detail in usage and reports
- Merge pull request #4 from tiagobalabuch/feature/log-phase-timing
- Refactor phase timing variables for clarity in DataClusteringAdvisor
- Update version to 0.3.0 and add timing for phases in DataClusteringAdvisor and row counting in warehouse_reader
- Merge pull request #3 from tiagobalabuch/changelog/v0.2.1
- Update CHANGELOG.md for v0.2.1
- Base changelog branch on master to avoid workflows permission issue
- Use force push for changelog branch
- Handle existing changelog branch in release workflow
- Add pull-requests write permission to release workflow
- Enhance release workflow to commit CHANGELOG.md and create a pull request for automated updates
- Fix release workflow to push to default branch and update predicate parser documentation. Fixing the write version number
- Merge pull request #2 from tiagobalabuch/DocumentationImprovements
- Update version to 0.3.0, enhance release workflow with changelog generation, and remove hybrid predicate extraction strategy
- Bump version to 0.2.0 in pyproject.toml
- Implement hybrid predicate extraction strategy using execution plans
- Add MkDocs configuration and documentation files for project deployment
- Renamed data clustering functions
- Bump version to 0.3.0
- Refactor get_table_row_counts function to remove redundant filtering logic
- Update documentation and function names to reflect Microsoft Fabric Data Warehouse terminology
- Merge pull request #1 from tiagobalabuch/docs-and-release-workflow
- Add in-repo documentation and GitHub Actions release workflow
- Initial commit: Fabric Warehouse Data Clustering Advisor v0.2.0

## [v0.3.0](https://github.com/tiagobalabuch/fabric-warehouse-advisor/releases/tag/v0.3.0)

- Merge pull request #7 from tiagobalabuch/feature/performance-advisor
- Bump version to 1.0.0, update installation instructions, and enhance version loading with warnings for better user feedback
- Enhance query regression checks to use window functions for median calculations and improve error handling in statistics health checks
- Refactor performance check phases to improve logging and timing, and enhance SQL identifier handling in statistics checks
- Update performance check documentation and version in pyproject.toml
- Refactor performance check findings to introduce new severity levels and update related logic for caching, collation, data types, and query regression checks
- Refactor table naming logic to enforce Warehouse identifier limits and enhance documentation for warehouse query functions with parameter details and retry logic
- Enhance table naming in recommendations to respect Warehouse identifier limits and add automatic retry logic with exponential back-off for warehouse table reads
- Update troubleshooting documentation to use placeholder for version in installation instructions
- Improve error handling in warehouse query reading and enhance version loading fallback
- Add query regression detection to Performance Check Advisor
- Add core modules for Fabric Warehouse Advisor
- Merge pull request #6 from tiagobalabuch/changelog/v0.3.0
- Update CHANGELOG.md for v0.3.0

## [v0.2.1](https://github.com/tiagobalabuch/fabric-warehouse-advisor/releases/tag/v0.2.1)

- Merge pull request #5 from tiagobalabuch/docs/readme-install-steps
- Improve documentation clarity in installation and how it works guides
- Update documentation for clarity and detail in usage and reports
- Merge pull request #4 from tiagobalabuch/feature/log-phase-timing
- Refactor phase timing variables for clarity in DataClusteringAdvisor
- Update version to 0.3.0 and add timing for phases in DataClusteringAdvisor and row counting in warehouse_reader
- Merge pull request #3 from tiagobalabuch/changelog/v0.2.1
- Update CHANGELOG.md for v0.2.1
- Base changelog branch on master to avoid workflows permission issue
- Use force push for changelog branch
- Handle existing changelog branch in release workflow

## [v0.2.0](https://github.com/tiagobalabuch/fabric-warehouse-advisor/releases/tag/v0.2.0)

- Add pull-requests write permission to release workflow
- Enhance release workflow to commit CHANGELOG.md and create a pull request for automated updates
- Fix release workflow to push to default branch and update predicate parser documentation. Fixing the write version number
- Merge pull request #2 from tiagobalabuch/DocumentationImprovements
- Update version to 0.3.0, enhance release workflow with changelog generation, and remove hybrid predicate extraction strategy

## [v0.1.0](https://github.com/tiagobalabuch/fabric-warehouse-advisor/releases/tag/v0.1.0)

- Bump version to 0.2.0 in pyproject.toml
- Implement hybrid predicate extraction strategy using execution plans
- Add MkDocs configuration and documentation files for project deployment

