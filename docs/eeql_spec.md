# EEQL spec (distilled)

Source: `eeql_spec.pdf`

## Core structure
```
select [first|last|nth(n)|all]? <event>
(
  <expr> as <alias>,
  ...
  [default_entity <entity>]
  [filter(<sql_where>)]
)
[ join [first|last|nth(n)|all]? <before|since|between|after|all> <event> (
    [using <entity1>, <entity2>, ...]
    <expr> as <alias>,
    ...
    [filter(<sql_where>)]
    [additional_join_expressions "<sql equality>"]
  )
]+
```

### Selectors
- Optional on select and join.
- `all` or omitted ⇒ every column must be aggregated.
- `nth(n)` supports arbitrary positive integer.

### Join qualifiers
`before | since | between | after | all`

### Column expressions
- Free-form SQL expressions; alias required.
- Aggregation functions validated against `vocabulary/aggregations.py`.

### default_entity / using
- `default_entity` only inside select block.
- `using` only inside join block; overrides select default_entity. If absent, join implicitly uses select default_entity.

### filter
- Appears inside parentheses; treated as raw SQL WHERE fragment.
- For joins, applies only to the joined event.

### additional_join_expressions
- Free-form SQL string(s) applied to the join predicate; currently stored and passed through.

### Semantic constraints
- Event names must exist.
- Entities referenced must exist on their event.
- Columns must exist on their event (best-effort detection).
- Aliases must be unique across whole query.
- Join uniqueness signature: (selector incl. omitted vs explicit, join qualifier, event, entities, filter, additional_join_expressions).
- Aggregation required when selector is `all` or omitted.

### Canonical examples
- Option A: join with selector, mixed aggregation.
- Option B: join without selector ⇒ all aggregated.
