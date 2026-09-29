---
name: project-documentation-workflow
description: >
  Autonomous end-to-end Project Documentation & PDF Generation Workflow.
  Scans codebase, extracts architecture, schemas, pipelines, ML models, and APIs,
  validates completeness, and compiles a comprehensive 31-section PDF document.
---

# Project Documentation & PDF Generation Workflow

A standardized, automated documentation workflow that extracts technical reality directly from the codebase (Kafka, Spark, Hive, Hadoop, MLlib, Docker, SQL, and UIs), performs an automated completeness audit, and compiles a publication-ready single-source-of-truth PDF document.

## Workflow Pipeline

```text
Scan Workspace ➔ Extract Metadata ➔ Generate 31 Sections ➔ Audit Completeness ➔ Build PDF
```

1. **Scan (`scan_project.py`):** Parses source files, SQL DDL, pipelines, ML metrics, Docker configs, and environment settings into `documentation/generated/project_metadata.json`.
2. **Generate (`generate_doc.py`):** Produces 31 adaptive Markdown chapters under `documentation/content/`.
3. **Validate (`validate_doc.py`):** Audits documentation against code artifacts, outputting `completeness_report.json` and `completeness_report.md`.
4. **Build PDF (`build_pdf.py`):** Compiles all sections using Python ReportLab with custom cover page, table of contents, headers/footers, styled tables, code blocks, and callout boxes into `documentation/generated/Project_Documentation.pdf`.

---

## Quick Start CLI

Run from the project root:

```powershell
# Full end-to-end documentation generation + audit + PDF export
python docs.py generate

# Or run individual sub-steps:
python docs.py scan       # Scan repository and extract metadata
python docs.py validate   # Run completeness audit
python docs.py pdf        # Recompile PDF from documentation/content/
```

---

## Output Artifacts

- **PDF Documentation:** `documentation/generated/Project_Documentation.pdf`
- **Completeness Report (Markdown):** `documentation/generated/completeness_report.md`
- **Completeness Report (JSON):** `documentation/generated/completeness_report.json`
- **Extracted Project Metadata:** `documentation/generated/project_metadata.json`
- **Modular Content Chapters:** `documentation/content/01_cover.md` through `31_appendix.md`

---

## 31-Section Document Hierarchy

1. Cover Page & Metadata
2. Document Control & Versioning
3. Table of Contents
4. Executive Summary
5. Project Overview & Problem Statement
6. Scope & Boundary Definitions
7. Users, Personas & Stakeholders
8. System Architecture (High & Component Level)
9. Technology Stack & Decision Matrix
10. Feature & Module Documentation
11. User & End-to-End Workflows
12. API & Service Interface Reference
13. Database Design & Hive Star Schema
14. The 20 Enterprise Hive BI Queries
15. Codebase Structure & Component Inventory
16. AI / ML Architecture & Spark MLlib Model
17. Security & Access Control
18. Offline, Resilience & Fault-Tolerance Handling
19. Developer Environment Setup Guide
20. Docker Deployment & Orchestration Guide
21. Testing Strategy & Test Matrix
22. Troubleshooting & Diagnostic Guide
23. Team Development Guidelines & Git Workflow
24. Team Responsibilities & Ownership Matrix
25. Technical Decision Log (ADRs)
26. Current Project Status & Milestones
27. Future Roadmap & Technical Debt
28. Glossary & Domain Terminology
29. Documentation Completeness Audit Report
30. Maintenance & Regeneration Workflow
31. Appendix & References
