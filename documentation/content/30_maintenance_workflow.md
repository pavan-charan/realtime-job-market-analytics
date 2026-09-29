# 30. Maintenance & Documentation Regeneration Workflow

Whenever the codebase, schemas, queries, or ML models are updated:

```powershell
# Run the autonomous documentation pipeline to update all markdown and re-render the PDF:
python docs.py generate
```

This ensures the documentation **never drifts from code reality**.
