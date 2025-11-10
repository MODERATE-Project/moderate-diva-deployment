# DQA Validator Processor

# StandardValidator YAML Configuration Documentation

## Overview

The StandardValidator uses a YAML configuration file to define data validation rules. The configuration consists of a list of rules, where each rule specifies a validation check to be performed on specific features of the input data.

## Configuration Structure

```yaml
rules:
  - name: <rule_name>
    feature: <feature_path>
    specs: <rule_specifications>
```

### Basic Elements

- `rules`: A list of validation rules
- `name`: The type of validation rule (required)
- `feature`: The path to the feature in the data sample (required)
- `specs`: Rule-specific parameters (required)

### Feature Path Specification

The `feature` field supports [JmesPath](https://jmespath.org/tutorial.html) notation to access nested fields:

- Simple field: `"field_name"`
- Nested field: `"parent.child"`
- Array field: `"array_field[*]"`

## Available Rules

### 1. Domain Rule

Validates if numeric values fall within specified integer ranges.

```yaml
rules:
  - name: domain
    feature: "age"
    specs:
      min: 18
      max: 65
```

**Specs Options:**

- `min`: Minimum allowed integer value (optional)
- `max`: Maximum allowed integer value (optional)

### 2. Data Type Rule

Validates the data type of values.

```yaml
rules:
  - name: datatype
    feature: "salary"
    specs:
      type: "FLOAT"
```

**Specs Options:**

- `type`: Required data type. Valid values:
  - "INTEGER"
  - "STRING"
  - "BOOLEAN"
  - "FLOAT"

### 3. Categorical Rule

Validates if values belong to a predefined list of values.

```yaml
rules:
  - name: categorical
    feature: "status"
    specs:
      values: ["active", "inactive", "pending"]
```

**Specs Options:**

- `values`: List of permitted string values (required)
- Default: No default list (-)

### 4. String Length Rule

Validates string length with multiple comparison options.

```yaml
rules:
  - name: strlen
    feature: "username"
    specs:
      len: 8
      lenType: "EXACT" # or "LOWER" or "UPPER"
```

**Specs Options:**

- `len`: Integer value for the length reference (required)
- `lenType`: Type of length comparison (required)
  - "EXACT": String must be exactly `len` characters
  - "LOWER": String must be shorter than `len` characters
  - "UPPER": String must be longer than `len` characters

### 5. Missing Rule

Validates missing values in the data.

```yaml
rules:
  - name: missing
    feature: "email"
    specs:
      exists: true
```

**Specs Options:**

- `exists`: Boolean, whether the feature is present or not (required)

### 6. Regular Expression Rule

Validates if string values match a specified [Python regular expression](https://docs.python.org/3/library/re.html) pattern.

```yaml
rules:
  - name: regex
    feature: "email"
    specs:
      regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
```

**Specs Options:**

- `regex`: Regular expression pattern string (required)

## Complete Example

```yaml
rules:
  - name: datatype
    feature: "user.age"
    specs:
      type: "INTEGER"

  - name: domain
    feature: "user.age"
    specs:
      min: 0
      max: 120

  - name: strlen
    feature: "user.name"
    specs:
      len: 50
      lenType: "UPPER"

  - name: regex
    feature: "user.email"
    specs:
      regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

  - name: categorical
    feature: "user.status"
    specs:
      values: ["active", "inactive", "suspended"]

  - name: missing
    feature: "user.contact_info"
    specs: {}
```

## Usage Notes

- Multiple rules can be applied to the same feature
- Rules are evaluated in the order they appear in the configuration
- All fields (`name`, `feature`, `specs`) are required for each rule
- Each rule generates a separate validation result in the output
- Default values are used when specifications are not provided (where applicable)
