# Attributes Processor

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `processor:attribute` (`urn:otel:processor:attribute`)
- Feature gate: Default
- Stability: Experimental

## Overview

The attributes processor mutates OpenTelemetry attributes in OTAP batches. It
supports Collector-style action lists for deleting, inserting, upserting,
updating, renaming, and hashing attributes.

The processor can apply actions to signal attributes, resource attributes,
scope attributes, or any combination of those domains.

## Getting Started

Start with an ordered action list and the attribute domains to mutate:

```yaml
type: processor:attribute
config:
  apply_to: ["resource", "signal"]
  actions:
    - action: upsert
      key: deployment.environment
      value:
        string: prod
    - action: hash
      key: user.email
      salt: "tenant-specific-salt"
```

## Configuration

```yaml
type: processor:attribute
config:
  # Attribute domains to mutate (default: ["signal"]).
  # Supported values are "signal", "resource", and "scope".
  apply_to: ["resource", "signal"]

  # Ordered list of attribute actions (default: []).
  actions:
    - action: delete
      key: temporary.attribute
    - action: insert
      key: deployment.environment
      value:
        string: prod
    - action: upsert
      key: service.namespace
      value:
        string: checkout
    - action: update
      key: service.version
      value:
        string: "1.2.3"
    - action: rename
      source_key: service
      destination_key: service.name
    - action: hash
      key: user.email
      algorithm: sha256
      salt: "tenant-specific-salt"
```

Supported `apply_to` values are `signal`, `resource`, and `scope`.

Supported actions:

- `delete` requires `key` and deletes an attribute.
- `insert` requires `key` and a value, and inserts it only when the key is
  absent.
- `upsert` requires `key` and a value, and inserts or replaces a value.
- `update` requires `key` and `value`, and replaces a value only when the key
  exists.
- `rename` requires `source_key` and `destination_key`, and renames an
  attribute key.
- `hash` requires `key` and replaces a scalar value with a salted hash.

`insert` and `upsert` take their value either from a literal `value` or from a
`from_attribute` reference to another attribute. Exactly one of the two must be
set. A `from_attribute` reference names the attribute set to read from with
`scope` (`resource`, `scope`, or `record`) and the attribute to read with `key`,
so a record attribute can be filled from the resource or scope that record
belongs to:

```yaml
actions:
  - action: insert
    key: instanceId
    from_attribute:
      scope: resource
      key: service.instance.id
```

Records whose referenced attribute is missing are left untouched.

`insert` and `upsert` also take an optional `condition` that restricts the
action to the records where another attribute equals a given value. It names the
attribute the same way a `from_attribute` reference does, plus the value to
compare against:

```yaml
actions:
  - action: insert
    key: componentName
    from_attribute:
      scope: scope
      key: node.id
    condition:
      scope: scope
      key: node.type
      equals: receiver
```

Records where the condition does not hold are left untouched, including records
whose tested attribute is missing.

A `from_attribute` reference may also carry a `pattern` and the `group` to read
from it, writing one named capture group of the read value instead of the whole
value. Set both fields or neither; the group must be declared by the pattern:

```yaml
actions:
  - action: insert
    key: pipelineName
    from_attribute:
      scope: scope
      key: flow.id
      pattern: "^(?P<pipelineName>[^/]+)/(?P<componentName>.+)$"
      group: pipelineName
```

Records whose value does not match the pattern are left untouched, so several
actions can read different groups of one pattern to split a single attribute
into several.

Actions of one kind compose into a single map keyed by attribute key, so two
`insert` actions (or two `upsert` actions) may not target the same key.

`hash.algorithm` defaults to `sha256`; `hash.salt` defaults to an empty string.
Unsupported action variants are accepted for forward compatibility and ignored.

## Examples

Rename a resource attribute:

```yaml
type: processor:attribute
config:
  apply_to: ["resource"]
  actions:
    - action: rename
      source_key: service
      destination_key: service.name
```

## Telemetry

These tables list telemetry emitted directly by this node. Common engine
runtime metric sets may also be attached by the pipeline telemetry policy.

### Metric Sets

#### `processor.attributes`

| Metric | Unit | Description |
| --- | --- | --- |
| `processor.attributes.transforms` | `{transform}` | Number of transform attempts, partitioned by `outcome` (`success` or `failure`). |
| `processor.attributes.modified.entries` | `{attr}` | Total number of attribute entries modified, partitioned by `action` and `domain`. |

### Events

| Event | Severity | Description |
| --- | --- | --- |
| *None* | N/A | No node-specific events are emitted. |

## Limits

- `apply_to` values other than `signal`, `resource`, or `scope` are rejected.
- Hashing supports scalar values and currently documents `sha256`.
- The same key cannot currently be used by more than one action in a single
  action list; see [issue #1036](https://github.com/open-telemetry/otel-arrow/issues/1036).
- Unsupported actions deserialize but are ignored.

## Related Docs

- [Configuration model](../../../../../docs/configuration-model.md)
- [Processor taxonomy](../../../../../docs/processors.md)
- [Core node catalog](../../../README.md)
