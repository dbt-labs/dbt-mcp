Rename a dbt model in the project workspace and automatically update all references.

This tool performs a complete model rename operation with the help of the LSP server:

1. **Queries the LSP server** for all files that reference the model being renamed
2. **Updates references** in all affected files (e.g., updates model names in `ref()` calls)
3. **Renames the model file** on disk to the new location
4. **Notifies the LSP server** that the rename is complete

The tool handles:
- Renaming model files (.sql files in your models directory)
- Updating all `ref('model_name')` references throughout the project
- Updating imports and dependencies in other models
- Preserving file encoding and line endings

**Important**: This tool makes actual changes to your files. Make sure you have committed any unsaved work before using it.

Use this when you need to safely rename a dbt model while ensuring all references to it are updated correctly throughout your dbt project.

