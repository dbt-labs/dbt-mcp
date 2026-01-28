# Lineage App Development Notes

This document captures issues encountered during development and their solutions.

## MCP ext-apps Integration

### MIME Type Validation (MCP SDK < 1.26.0)

**Problem:** FastMCP's `Resource` base class had a strict regex pattern for MIME types that didn't allow parameters:
```python
pattern=r"^[a-zA-Z0-9]+/[a-zA-Z0-9\-+.]+$"
```

The MCP ext-apps specification requires `text/html;profile=mcp-app` for UI resources, but the `;` character was rejected by Pydantic validation:
```
pydantic_core._pydantic_core.ValidationError: 1 validation error for FunctionResource
mime_type
  String should match pattern '^[a-zA-Z0-9]+/[a-zA-Z0-9\-+.]+$'
```

**Solution:** Upgrade to MCP SDK 1.26.0+ which updated the pattern to allow MIME type parameters:
```python
pattern=r"^[a-zA-Z0-9]+/[a-zA-Z0-9\-+.]+(;\s*[a-zA-Z0-9\-_.]+=[a-zA-Z0-9\-_.]+)*$"
```

### Structured Output Return Type

**Problem:** When using `structured_output=True` on a tool, the MCP SDK requires a TypedDict return type, not a plain `dict`. Returning `dict` caused:
```
InvalidSignature: return type dict is not serializable for structured output
```

**Solution:** Define explicit TypedDict classes for the return value:
```python
class LineageNode(TypedDict):
    uniqueId: str
    name: str
    resourceType: str
    parentIds: NotRequired[list[str]]

class LineageVisualizationResult(TypedDict):
    targetId: str
    nodes: list[LineageNode]
```

### Optional Fields in TypedDict

**Problem:** Sources and Seeds don't have parent dependencies, so `parentIds` was missing from some nodes. This caused Pydantic validation errors:
```
Field required [type=missing, input_value={'uniqueId': '...', 'name': '...'}, input_type=dict]
```

**Solution:** Use `NotRequired` from `typing` (Python 3.11+) to mark optional fields:
```python
from typing import NotRequired, TypedDict

class LineageNode(TypedDict):
    uniqueId: str
    name: str
    resourceType: str
    parentIds: NotRequired[list[str]]  # Optional for Sources/Seeds
```

## React/TypeScript Issues

### Handling Optional parentIds in Frontend

**Problem:** TypeScript type declared `parentIds: string[]` as required, but the backend made it optional. This caused runtime errors when accessing `node.parentIds.forEach(...)` on nodes without parents.

**Solution:**
1. Update TypeScript type to match backend:
   ```typescript
   export interface LineageNode {
     uniqueId: string;
     name: string;
     resourceType: ResourceType;
     parentIds?: string[];  // Optional
   }
   ```

2. Use nullish coalescing when iterating:
   ```typescript
   (node.parentIds ?? []).forEach((parentId) => { ... });
   ```

### App Container Height

**Problem:** The embedded app appeared as a thin black rectangle because the MCP host didn't provide explicit height constraints.

**Solution:** Set minimum height on root elements:
```css
html, body, #root {
  min-height: 500px;
}

.app-container {
  min-height: 500px;
}
```

## Response Size

**Problem:** Initial implementation with `depth=5` and all resource types returned ~2.8MB of data, causing "visualization was too large" errors.

**Solution:**
- Reduced default depth from 5 to 2
- Excluded Test nodes by default (they add significant volume)
- Added `include_tests` parameter for users who need them

## Build Configuration

### Single-File HTML Bundle

The app uses `vite-plugin-singlefile` to bundle everything (JS, CSS, assets) into a single HTML file. This is required because MCP ext-apps resources must be self-contained.

```typescript
// vite.config.ts
import { viteSingleFile } from "vite-plugin-singlefile";

export default defineConfig({
  plugins: [react(), viteSingleFile()],
  build: {
    target: "esnext",
    assetsInlineLimit: 100000000,
    cssCodeSplit: false,
  },
});
```
