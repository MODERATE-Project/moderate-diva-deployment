import csv
import hashlib
import io
import json
import random
from enum import Enum
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Set

import yaml
import math
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import (
    ExpressionLanguageScope,
    PropertyDescriptor,
    StandardValidators,
)
from nifiapi.relationship import Relationship

# =============================================================================
# SAMPLING CONFIGURATION
# =============================================================================
# These settings control how many records are read from the dataset to infer
# validation rules. Sampling is essential because reading entire large datasets
# would be slow and memory-intensive; a representative sample is usually enough.
#
# HOW IT WORKS:
#   target_sample = clamp(dataset_size * PERCENT / 100, MIN, MAX)
#
# EXAMPLES:
#   - 500 rows at 10% => 50, but MIN=200 lifts it to 200.
#   - 10,000 rows at 10% => 1,000 (within bounds, used as-is).
#   - 200,000 rows at 10% => 20,000, but MAX=5000 caps it to 5,000.
#
# TUNING:
#   - Increase PERCENT or MAX for better coverage on diverse datasets.
#   - Decrease PERCENT or MAX to reduce CPU/memory on very large files.
#   - Raise MIN if small datasets need more thorough sampling.
# -----------------------------------------------------------------------------
DEFAULT_SAMPLE_PERCENT = 10
DEFAULT_MIN_SAMPLE_SIZE = 200
DEFAULT_MAX_SAMPLE_SIZE = 5000
# Legacy fallback when percent-based sampling is unavailable (safety net).
DEFAULT_SAMPLE_SIZE = 1000

# =============================================================================
# CATEGORICAL RULE SETTINGS
# =============================================================================
# A "categorical" rule restricts a field to a fixed set of allowed values
# (e.g., status in ["pending", "approved", "rejected"]).
#
# MAX_CATEGORIES: if a field has MORE distinct values than this, no categorical
#   rule is emitted (the field is considered free-form, not an enum).
#   - Higher value: more fields treated as categorical (larger allowed-value lists).
#   - Lower value: fewer categorical rules; only very low-cardinality fields qualify.
#
# EXAMPLE: MAX_CATEGORIES=20 means a field with 25 distinct values won't get a
#   categorical rule, but one with 15 distinct values will.
# -----------------------------------------------------------------------------
DEFAULT_MAX_CATEGORIES = 20

# =============================================================================
# REGEX DERIVATION
# =============================================================================
# When enabled ("true"), the processor attempts to infer a regex pattern for
# string fields that share a uniform shape (e.g., dates like "2024-01-15").
#
# WHY DISABLED BY DEFAULT: with small samples, a regex can overfit to a few
#   examples and reject valid data that differs slightly. Enable only if you
#   expect consistent string formats and have sufficient sample sizes.
# -----------------------------------------------------------------------------
DEFAULT_REGEX_DERIVATION = "false"

# =============================================================================
# PERMISSIVE NUMERIC CHECKS
# =============================================================================
# Controls how strict numeric (INTEGER/FLOAT) validation rules are.
#
# WHEN TRUE (default, permissive):
#   - Domain ranges are widened by a buffer (30% of observed span, min 3 units)
#     to tolerate natural data drift beyond the sampled min/max.
#   - Numeric strings like "123.4" are coerced and accepted as valid numbers.
#
# WHEN FALSE (strict):
#   - Domain rules use the exact observed min/max with no buffer.
#   - Only native numeric types pass; strings like "123" fail datatype checks.
#
# RECOMMENDATION: keep TRUE unless you need exact numeric enforcement.
# -----------------------------------------------------------------------------
DEFAULT_PERMISSIVE_NUMERIC_CHECKS = "true"

# =============================================================================
# THRESHOLD FRACTIONS
# =============================================================================
# These fractions determine how much evidence (observations) is required before
# the processor emits categorical, regex, or domain rules. They are applied to
# the SAMPLED record count, not the full dataset.
#
# FORMULA: required_observations = sampled_count * FRACTION (then clamped)
#
# CATEGORICAL_FRACTION:
#   How many distinct values must appear before emitting a categorical rule.
#   Example: 1,000 sampled records * 0.20 = 200 distinct values required.
#   - Raise to demand more diversity before locking in allowed values.
#   - Lower to emit categorical rules sooner (riskier on small samples).
#
# REGEX_FRACTION:
#   How many samples must share a uniform shape before emitting a regex rule.
#   Example: 1,000 sampled records * 0.20 = 200 samples with matching shape.
#   - Raise to require stronger pattern evidence.
#   - Lower to emit regexes sooner (may overfit).
#
# DOMAIN_FRACTION:
#   How many numeric observations are needed before emitting a min/max domain
#   rule. Until this threshold is met, no domain rule is generated (avoids
#   brittle ranges from sparse numeric data).
#   Example: 1,000 sampled records * 0.40 = 400 numeric values required.
#   - Raise to require more numeric evidence before trusting observed min/max.
#   - Lower to emit domain rules sooner (riskier tight bounds).
# -----------------------------------------------------------------------------
CATEGORICAL_FRACTION = 0.20
REGEX_FRACTION = 0.20
DOMAIN_FRACTION = 0.40

# =============================================================================
# THRESHOLD CLAMPS (MIN/MAX BOUNDS)
# =============================================================================
# After applying the fractions above, the computed thresholds are clamped to
# these bounds to prevent extremes on very small or very large samples.
#
# WHY NEEDED:
#   - On tiny samples (e.g., 50 records), fractions yield tiny thresholds that
#     would emit rules from insufficient evidence. MIN bounds ensure a baseline.
#   - On huge samples (e.g., 100,000 records), fractions yield huge thresholds
#     that might never be met. MAX bounds cap requirements to realistic levels.
#
# CATEGORICAL bounds: require at least MIN_CATEGORICAL_UNIQUE_MIN distinct
#   values, but no more than MIN_CATEGORICAL_UNIQUE_MAX.
# REGEX bounds: require at least MIN_REGEX_SAMPLES_MIN matching samples.
# DOMAIN bounds: require at least MIN_DOMAIN_SAMPLES_MIN numeric observations.
#
# EXAMPLE with 100 sampled records and CATEGORICAL_FRACTION=0.20:
#   Computed = 100 * 0.20 = 20, within [5, 100], so threshold = 20.
# EXAMPLE with 20 sampled records:
#   Computed = 20 * 0.20 = 4, below MIN=5, so threshold = 5.
# -----------------------------------------------------------------------------
MIN_CATEGORICAL_UNIQUE_MIN = 5
MIN_CATEGORICAL_UNIQUE_MAX = 100
MIN_REGEX_SAMPLES_MIN = 5
MIN_REGEX_SAMPLES_MAX = 100
MIN_DOMAIN_SAMPLES_MIN = 30
MIN_DOMAIN_SAMPLES_MAX = 500

# =============================================================================
# ENCAPSULATOR METADATA FIELDS
# =============================================================================
# When UnifiedDataModelEncapsulator is in the flow, it wraps the original
# payload under "metricValue" and adds envelope metadata (timestamp, sourceType,
# etc.). These fields are auto-generated and should NOT have validation rules
# inferred from them—only the user's actual data under metricValue.* matters.
#
# The processor detects encapsulated records (presence of "metricValue" key)
# and skips these top-level envelope fields during stats collection.
# -----------------------------------------------------------------------------
ENCAPSULATOR_META_FIELDS = {
    "timestamp",
    "sourceType",
    "sourceID",
    "infoType",
    "dataType",
    "dataItemID",
    "metricTypeID",
    "metricValue",
}

# =============================================================================
# INTERNAL CONSTANTS (rarely need adjustment)
# =============================================================================
# CONTENT_HASH_PREFIX_LEN: bytes of content used for fallback fingerprinting
#   when JSON parsing fails.
# ISO_DATE_*: positions for lightweight YYYY-MM-DD date detection heuristic.
# -----------------------------------------------------------------------------
CONTENT_HASH_PREFIX_LEN = 128
ISO_DATE_LENGTH = 10
ISO_DATE_FIRST_DASH_POS = 4
ISO_DATE_SECOND_DASH_POS = 7


def _compute_sample_target(
    total_records: int,
    sample_percent: int,
    min_sample_size: int,
    max_sample_size: int,
    fallback_sample_size: int,
) -> int:
    """
    Compute a bounded sample target based on dataset size. When total_records is
    small, percent-based target may be below min_sample_size; when very large,
    clamp to max_sample_size. Fallback is used if percent produces zero.
    """
    target = math.ceil(total_records * (sample_percent / 100))
    target = max(min_sample_size, target)
    target = min(max_sample_size, target)
    if target <= 0:
        target = fallback_sample_size
    return target


class Format(str, Enum):
    """Supported formats for the rule builder."""

    AUTO = "AUTO"
    CSV = "CSV"
    JSON = "JSON"


class TypeLabel(str, Enum):
    """Output type labels used in DQA rules."""

    BOOLEAN = "BOOLEAN"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    STRING = "STRING"


class InferredType(str, Enum):
    """Internal types used during inference."""

    BOOLEAN = "bool"
    INTEGER = "int"
    FLOAT = "float"
    STRING = "string"


def _hash_string(value: str) -> str:
    """Stable SHA-256 hex helper for fingerprints."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class FormatHandler:
    """Interface for format-specific detection, parsing, and fingerprinting."""

    name: Format = Format.CSV

    def detect(self, content: str) -> bool:
        """Return True when this handler should process the content."""
        raise NotImplementedError

    def fingerprint(self, content: str) -> str:
        """Derive a format-specific fingerprint to detect schema changes."""
        raise NotImplementedError

    def parse_records(
        self,
        content: str,
        sample_percent: int,
        min_sample_size: int,
        max_sample_size: int,
        fallback_sample_size: int,
    ) -> List[Dict[str, Any]]:
        """Parse up to sample_size records into dictionaries."""
        raise NotImplementedError


class CsvHandler(FormatHandler):
    name = Format.CSV

    def detect(self, content: str) -> bool:
        """CSV is the default when JSON detection fails."""
        return True  # default when not JSON

    def fingerprint(self, content: str) -> str:
        """Hash the header line to detect column changes."""
        header = content.splitlines()[0] if content else ""
        return _hash_string(header)

    def parse_records(
        self,
        content: str,
        sample_percent: int,
        min_sample_size: int,
        max_sample_size: int,
        fallback_sample_size: int,
    ) -> List[Dict[str, Any]]:
        """
        Parse CSV rows into dicts using reservoir sampling to avoid loading the
        entire dataset into memory while still providing an unbiased random
        sample.
        """
        reservoir: List[Dict[str, Any]] = []
        for idx, row in enumerate(csv.DictReader(io.StringIO(content)), start=1):
            target = _compute_sample_target(
                idx,
                sample_percent,
                min_sample_size,
                max_sample_size,
                fallback_sample_size,
            )
            if len(reservoir) < target:
                reservoir.append(row)
            else:
                j = random.randint(1, idx)
                if j <= target:
                    reservoir[j - 1] = row
        return reservoir


class JsonHandler(FormatHandler):
    name = Format.JSON

    def detect(self, content: str) -> bool:
        """Detect JSON by leading object/array markers."""
        trimmed = content.lstrip()
        return trimmed.startswith("{") or trimmed.startswith("[")

    def fingerprint(self, content: str) -> str:
        """Hash top-level keys (first element for arrays) to catch schema shifts."""
        try:
            data = json.loads(content)
            if isinstance(data, list) and data:
                keys = sorted(data[0].keys())
            elif isinstance(data, dict):
                keys = sorted(data.keys())
            else:
                keys = []
            return _hash_string("|".join(keys))
        except Exception:
            return _hash_string(content[:CONTENT_HASH_PREFIX_LEN])

    def parse_records(
        self,
        content: str,
        sample_percent: int,
        min_sample_size: int,
        max_sample_size: int,
        fallback_sample_size: int,
    ) -> List[Dict[str, Any]]:
        """Parse dict or list payload into a bounded, percent-based sample."""
        data = json.loads(content)
        if isinstance(data, dict):
            return [data]
        if isinstance(data, list):
            if not data:
                return []
            target = _compute_sample_target(
                len(data),
                sample_percent,
                min_sample_size,
                max_sample_size,
                fallback_sample_size,
            )
            k = min(target, len(data))
            return random.sample(data, k)
        raise ValueError("Unsupported JSON structure")


class RuleBuilderProcessor(FlowFileTransform):
    """
    A NiFi FlowFileTransform that infers lightweight validation rules
    from the head of CSV or JSON datasets and writes them to FlowFile
    attributes so downstream validators (e.g., DQAValidator) can apply
    per-dataset rules.
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "2.0.0-M4"
        description = "Builds dynamic DQA rules from CSV/JSON samples"
        tags = ["dqa", "rule-builder", "links"]
        dependencies = ["pyyaml==6.0.1"]

    # Properties
    SAMPLE_SIZE = PropertyDescriptor(
        name="Sample Size",
        description="Legacy absolute cap on records to sample when inferring rules",
        required=True,
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=str(DEFAULT_SAMPLE_SIZE),
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    )

    SAMPLE_PERCENT = PropertyDescriptor(
        name="Sample Size Percent",
        description=(
            "Percent of records to sample (bounded by Min/Max Sample Size) when "
            "inferring rules"
        ),
        required=True,
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=str(DEFAULT_SAMPLE_PERCENT),
        expression_language_scope=ExpressionLanguageScope.NONE,
    )

    MIN_SAMPLE_SIZE = PropertyDescriptor(
        name="Min Sample Size",
        description="Lower bound on sampled records regardless of dataset size",
        required=True,
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=str(DEFAULT_MIN_SAMPLE_SIZE),
        expression_language_scope=ExpressionLanguageScope.NONE,
    )

    MAX_SAMPLE_SIZE = PropertyDescriptor(
        name="Max Sample Size",
        description="Upper bound on sampled records regardless of dataset size",
        required=True,
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=str(DEFAULT_MAX_SAMPLE_SIZE),
        expression_language_scope=ExpressionLanguageScope.NONE,
    )

    MAX_CATEGORIES = PropertyDescriptor(
        name="Max Categories",
        description="Maximum unique values to treat as categorical",
        required=True,
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=str(DEFAULT_MAX_CATEGORIES),
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    )

    REGEX_DERIVATION = PropertyDescriptor(
        name="Regex Derivation",
        description="Whether to derive simple regexes for string fields",
        required=True,
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
        default_value=DEFAULT_REGEX_DERIVATION,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    )

    PERMISSIVE_NUMERIC_CHECKS = PropertyDescriptor(
        name="Permissive Numeric Checks",
        description=(
            "If true, relax numeric ranges and allow coercible numeric strings; "
            "if false, keep exact min/max and require native numeric types"
        ),
        required=True,
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
        default_value=DEFAULT_PERMISSIVE_NUMERIC_CHECKS,
        expression_language_scope=ExpressionLanguageScope.NONE,
    )

    DATASET_ID_ATTR = PropertyDescriptor(
        name="Dataset ID Attribute",
        description="Attribute name holding the dataset identifier",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value="dataset.id",
        expression_language_scope=ExpressionLanguageScope.NONE,
    )

    FINGERPRINT_ATTR = PropertyDescriptor(
        name="Fingerprint Attribute",
        description="Attribute name holding a dataset fingerprint/hash",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value="dataset.fingerprint",
        expression_language_scope=ExpressionLanguageScope.NONE,
    )

    FORMAT = PropertyDescriptor(
        name="Format",
        description="Force format (AUTO, CSV, JSON)",
        required=True,
        allowable_values=[Format.AUTO.value, Format.CSV.value, Format.JSON.value],
        default_value=Format.AUTO.value,
        expression_language_scope=ExpressionLanguageScope.NONE,
    )

    # Relationships
    SUCCESS_REL = Relationship(name="success", description="Rules inferred")
    FAILURE_REL = Relationship(name="failure", description="Failed to infer rules")

    descriptors = [
        SAMPLE_SIZE,
        SAMPLE_PERCENT,
        MIN_SAMPLE_SIZE,
        MAX_SAMPLE_SIZE,
        MAX_CATEGORIES,
        REGEX_DERIVATION,
        PERMISSIVE_NUMERIC_CHECKS,
        DATASET_ID_ATTR,
        FINGERPRINT_ATTR,
        FORMAT,
    ]

    relationships = [SUCCESS_REL, FAILURE_REL]

    def __init__(self, **kwargs):
        pass

    def onScheduled(self, context):
        """Load processor configuration and initialize caches/handlers."""
        self.sample_size = int(
            context.getProperty(self.SAMPLE_SIZE)
            .evaluateAttributeExpressions()
            .getValue()
        )
        self.sample_percent = int(context.getProperty(self.SAMPLE_PERCENT).getValue())
        self.min_sample_size = int(context.getProperty(self.MIN_SAMPLE_SIZE).getValue())
        self.max_sample_size = int(context.getProperty(self.MAX_SAMPLE_SIZE).getValue())
        self.max_categories = int(
            context.getProperty(self.MAX_CATEGORIES)
            .evaluateAttributeExpressions()
            .getValue()
        )
        self.regex_derivation = (
            context.getProperty(self.REGEX_DERIVATION)
            .evaluateAttributeExpressions()
            .asBoolean()
        )
        self.permissive_numeric_checks = context.getProperty(
            self.PERMISSIVE_NUMERIC_CHECKS
        ).asBoolean()
        self.dataset_id_attr = context.getProperty(self.DATASET_ID_ATTR).getValue()
        self.fingerprint_attr = context.getProperty(self.FINGERPRINT_ATTR).getValue()
        self.format_setting = (
            context.getProperty(self.FORMAT).getValue() or Format.AUTO.value
        )
        # In-memory cache keyed by dataset+fingerprint to avoid re-sampling in the same JVM
        self._rule_cache: Dict[str, str] = {}
        self._handlers = {
            Format.JSON: JsonHandler(),
            Format.CSV: CsvHandler(),
        }

    def getPropertyDescriptors(self):
        """Expose processor properties to NiFi."""
        return self.descriptors

    def getRelationships(self):
        """Expose processor relationships to NiFi."""
        return self.relationships

    def transform(self, context, flowfile):
        """Infer rules for a FlowFile, cache them, and emit enriched attributes."""
        try:
            content_bytes = flowfile.getContentsAsBytes()
            content_str = content_bytes.decode("utf-8")
            attrs = flowfile.getAttributes()

            dataset_id = attrs.get(self.dataset_id_attr, "default-dataset")
            fingerprint = attrs.get(self.fingerprint_attr)
            fmt = self._detect_format(content_str, self.format_setting)
            handler = self._get_handler(fmt)

            # Use content-derived fingerprint if missing
            if not fingerprint:
                fingerprint = handler.fingerprint(content_str)

            cache_key = f"{dataset_id}:{fingerprint}"

            if cache_key in self._rule_cache:
                rule_yaml = self._rule_cache[cache_key]
            else:
                records = handler.parse_records(
                    content_str,
                    self.sample_percent,
                    self.min_sample_size,
                    self.max_sample_size,
                    self.sample_size,
                )
                sampled_count = len(records)
                thresholds = self._derive_thresholds(sampled_count)
                stats = self._collect_stats(records)
                rule_yaml = self._build_rules_yaml(stats, thresholds)
                self._rule_cache[cache_key] = rule_yaml

            new_attrs = dict(attrs)
            new_attrs["dqa.rules"] = rule_yaml
            new_attrs["dqa.version"] = fingerprint
            new_attrs[self.dataset_id_attr] = dataset_id
            new_attrs[self.fingerprint_attr] = fingerprint
            new_attrs["dqa.format"] = fmt.value

            return FlowFileTransformResult(
                relationship="success", attributes=new_attrs, contents=content_bytes
            )
        except Exception as exc:
            self.logger.error(f"RuleBuilderProcessor failed: {exc}")
            return FlowFileTransformResult(relationship="failure")

    def _detect_format(self, content: str, setting: str) -> Format:
        """Resolve format from setting or handler detection."""
        if setting and setting != Format.AUTO.value:
            try:
                return Format(setting)
            except ValueError:
                return Format.CSV
        for handler in self._handlers.values():
            if handler.detect(content):
                return handler.name
        return Format.CSV

    def _get_handler(self, fmt: str) -> FormatHandler:
        """Return the handler for the requested format (CSV fallback)."""
        handler = self._handlers.get(fmt)
        if handler:
            return handler
        # Fallback to CSV to avoid hard failure on unknown format setting
        return self._handlers[Format.CSV]

    def _collect_stats(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate per-field stats used to derive rules."""
        field_stats: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {
                "count": 0,
                "missing": 0,
                "type_counts": Counter(),
                "categories": set(),  # type: Set[Any]
                "numeric_min": None,
                "numeric_max": None,
                "numeric_count": 0,
                "samples": [],
            }
        )

        for record in records:
            # Detect encapsulated records so we can omit envelope metadata
            # (timestamp/sourceType/metricValue, etc.) from rule inference.
            has_encapsulator = isinstance(record, dict) and "metricValue" in record
            flat_record = self._flatten_record(record)
            for field, value in flat_record.items():
                if has_encapsulator and self._is_encapsulator_meta_field(field):
                    # Skip envelope-only fields; keep metricValue.* payload fields.
                    continue
                stats = field_stats[field]
                if value is None:
                    stats["missing"] += 1
                    continue
                if isinstance(value, str):
                    val = value.strip()
                    if val == "":
                        stats["missing"] += 1
                        continue
                    value = val
                stats["count"] += 1
                inferred = self._infer_type(value)
                stats["type_counts"][inferred] += 1
                if inferred in (InferredType.INTEGER.value, InferredType.FLOAT.value):
                    num_val = float(value)
                    stats["numeric_count"] += 1
                    stats["numeric_min"] = (
                        num_val
                        if stats["numeric_min"] is None
                        else min(stats["numeric_min"], num_val)
                    )
                    stats["numeric_max"] = (
                        num_val
                        if stats["numeric_max"] is None
                        else max(stats["numeric_max"], num_val)
                    )
                else:
                    if len(stats["categories"]) < self.max_categories:
                        stats["categories"].add(value)
                if (
                    self.regex_derivation
                    and len(stats["samples"]) < self.max_categories
                ):
                    stats["samples"].append(value)

        return field_stats

    def _is_encapsulator_meta_field(self, field: str) -> bool:
        """
        Skip metadata fields introduced by UnifiedDataModelEncapsulator when it
        is present in the flow (detected via metricValue root key).
        """
        root = field.split(".", 1)[0]
        return root in ENCAPSULATOR_META_FIELDS and not field.startswith("metricValue.")

    def _flatten_record(self, record: Any, prefix: str = "") -> Dict[str, Any]:
        """
        Flatten nested dicts into dotted keys so generated feature paths work
        with jmespath in the validator (e.g., metricValue.Total_Power).
        Lists are left as-is under the current key to avoid exploding paths.
        """
        if not isinstance(record, dict):
            return {prefix or "value": record}

        flat: Dict[str, Any] = {}
        for key, value in record.items():
            new_prefix = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                flat.update(self._flatten_record(value, new_prefix))
            else:
                flat[new_prefix] = value
        return flat

    def _infer_type(self, value: Any) -> str:
        """Best-effort primitive type inference for a single value."""
        if isinstance(value, bool):
            return InferredType.BOOLEAN.value
        if isinstance(value, (int, float)):
            return (
                InferredType.FLOAT.value
                if isinstance(value, float)
                else InferredType.INTEGER.value
            )
        # strings from CSV need parsing
        if isinstance(value, str):
            lowered = value.lower()
            if lowered in ("true", "false"):
                return InferredType.BOOLEAN.value
            try:
                int(value)
                return InferredType.INTEGER.value
            except Exception:
                pass
            try:
                float(value)
                return InferredType.FLOAT.value
            except Exception:
                pass
        return InferredType.STRING.value

    def _choose_type(self, type_counts: Counter) -> str:
        """Map inferred counters to DQA datatype label."""
        if not type_counts:
            return TypeLabel.STRING.value
        preferred = type_counts.most_common(1)[0][0]
        if preferred == InferredType.BOOLEAN.value:
            return TypeLabel.BOOLEAN.value
        if preferred == InferredType.INTEGER.value:
            return TypeLabel.INTEGER.value
        if preferred == InferredType.FLOAT.value:
            return TypeLabel.FLOAT.value
        return TypeLabel.STRING.value

    def _derive_regex(self, samples: List[Any]) -> Optional[str]:
        """Derive a simple regex when samples are uniform in shape."""
        str_samples = [
            str(s) for s in samples if isinstance(s, (str, int, float, bool))
        ]
        if not str_samples:
            return None
        lengths = {len(s) for s in str_samples}
        if len(lengths) == 1:
            # If all are digits -> numeric pattern
            if all(s.isdigit() for s in str_samples):
                return r"^\d{" + str(len(str_samples[0])) + r"}$"
        # Date-ish ISO patterns
        if all(self._looks_iso_date(s) for s in str_samples):
            return r"^\d{4}-\d{2}-\d{2}"
        return None

    def _looks_iso_date(self, value: str) -> bool:
        """Lightweight check for YYYY-MM-DD prefix."""
        if len(value) < ISO_DATE_LENGTH:
            return False
        prefix = value[:ISO_DATE_LENGTH]
        return bool(
            len(prefix) == ISO_DATE_LENGTH
            and prefix[ISO_DATE_FIRST_DASH_POS] == "-"
            and prefix[ISO_DATE_SECOND_DASH_POS] == "-"
            and prefix[:ISO_DATE_FIRST_DASH_POS].isdigit()
            and prefix[ISO_DATE_FIRST_DASH_POS + 1 : ISO_DATE_SECOND_DASH_POS].isdigit()
            and prefix[ISO_DATE_SECOND_DASH_POS + 1 : ISO_DATE_LENGTH].isdigit()
        )

    def _looks_datetime_field(self, field: str, samples: List[Any]) -> bool:
        """
        Heuristic: treat fields as datetime-like when the name hints it or when
        sample values look like ISO dates. Used to avoid over-constraining with
        categorical rules on high-cardinality timestamps.
        """
        lowered = field.lower()
        if "time" in lowered or "date" in lowered:
            return True
        return any(self._looks_iso_date(str(s)) for s in samples)

    def _relax_range(self, min_val: float, max_val: float, dqa_type: str):
        """
        Relax numeric ranges when strict checks are disabled.

        This widens the observed bounds by 30% of the span with a minimum
        buffer of 3 to avoid brittle min/max rules when data drifts slightly.
        """
        if not self.permissive_numeric_checks:
            return min_val, max_val
        span = max_val - min_val
        buffer = max(3.0, abs(span) * 0.3)
        relaxed_min = min_val - buffer
        relaxed_max = max_val + buffer
        if dqa_type == TypeLabel.INTEGER.value:
            return int(relaxed_min), int(relaxed_max)
        return relaxed_min, relaxed_max

    def _derive_thresholds(self, sampled_count: int) -> Dict[str, int]:
        """
        Derive thresholds from the sampled count with sane bounds to reduce
        brittleness while keeping rules meaningful.
        """

        def clamp(value: int, lower: int, upper: int) -> int:
            return max(lower, min(upper, value))

        cat = clamp(
            math.ceil(sampled_count * CATEGORICAL_FRACTION),
            MIN_CATEGORICAL_UNIQUE_MIN,
            MIN_CATEGORICAL_UNIQUE_MAX,
        )
        regex = clamp(
            math.ceil(sampled_count * REGEX_FRACTION),
            MIN_REGEX_SAMPLES_MIN,
            MIN_REGEX_SAMPLES_MAX,
        )
        domain = clamp(
            math.ceil(sampled_count * DOMAIN_FRACTION),
            MIN_DOMAIN_SAMPLES_MIN,
            MIN_DOMAIN_SAMPLES_MAX,
        )
        return {
            "min_categorical_unique": cat,
            "min_regex_samples": regex,
            "min_domain_samples": domain,
        }

    def _build_rules_yaml(
        self, stats: Dict[str, Any], thresholds: Dict[str, int]
    ) -> str:
        """Convert collected stats into YAML rule definitions."""
        rules = []
        for field, s in stats.items():
            total_seen = s["count"] + s["missing"]
            if total_seen == 0:
                continue
            dqa_type = self._choose_type(s["type_counts"])
            min_categorical_unique = thresholds["min_categorical_unique"]
            min_regex_samples = thresholds["min_regex_samples"]
            min_domain_samples = thresholds["min_domain_samples"]

            # required/optional
            rules.append(
                {
                    "name": "exists",
                    "feature": field,
                    "specs": {"exists": s["missing"] == 0},
                }
            )

            # datatype rule
            datatype_specs: Dict[str, Any] = {"type": dqa_type}
            if self.permissive_numeric_checks and dqa_type in (
                TypeLabel.INTEGER.value,
                TypeLabel.FLOAT.value,
            ):
                datatype_specs["coerce_numeric_strings"] = True
            rules.append(
                {
                    "name": "datatype",
                    "feature": field,
                    "specs": datatype_specs,
                }
            )

            # domain for numerics
            if (
                dqa_type in (TypeLabel.INTEGER.value, TypeLabel.FLOAT.value)
                and s["numeric_min"] is not None
                and s["numeric_max"] is not None
                and s["numeric_count"] >= min_domain_samples
            ):
                relaxed_min, relaxed_max = self._relax_range(
                    s["numeric_min"], s["numeric_max"], dqa_type
                )
                domain_specs: Dict[str, Any] = {"min": relaxed_min, "max": relaxed_max}
                if self.permissive_numeric_checks:
                    domain_specs["coerce_numeric_strings"] = True
                rules.append(
                    {
                        "name": "domain",
                        "feature": field,
                        "specs": domain_specs,
                    }
                )

            # categorical if small distinct
            if (
                s["categories"]
                and len(s["categories"]) <= self.max_categories
                and len(s["categories"]) >= min_categorical_unique
                and not self._looks_datetime_field(field, s["samples"])
            ):
                rules.append(
                    {
                        "name": "categorical",
                        "feature": field,
                        "specs": {"values": list(s["categories"])},
                    }
                )

            # optional regex
            if self.regex_derivation and len(s["samples"]) >= min_regex_samples:
                regex = self._derive_regex(s["samples"])
                if regex:
                    rules.append(
                        {
                            "name": "regex",
                            "feature": field,
                            "specs": {"regex": regex},
                        }
                    )

        return yaml.safe_dump({"rules": rules}, sort_keys=False)
