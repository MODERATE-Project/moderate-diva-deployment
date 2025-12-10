import csv
import hashlib
import io
import json
from enum import Enum
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Set

import yaml
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import (
    ExpressionLanguageScope,
    PropertyDescriptor,
    StandardValidators,
)
from nifiapi.relationship import Relationship

# Defaults and reused literals kept in one place to avoid magic numbers.
DEFAULT_SAMPLE_SIZE = 100
DEFAULT_MAX_CATEGORIES = 20
DEFAULT_REGEX_DERIVATION = "false"
CONTENT_HASH_PREFIX_LEN = 128
ISO_DATE_LENGTH = 10
ISO_DATE_FIRST_DASH_POS = 4
ISO_DATE_SECOND_DASH_POS = 7


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

    def parse_records(self, content: str, sample_size: int) -> List[Dict[str, Any]]:
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

    def parse_records(self, content: str, sample_size: int) -> List[Dict[str, Any]]:
        """Parse CSV rows into dicts, capped by sample_size."""
        reader = csv.DictReader(io.StringIO(content))
        records: List[Dict[str, Any]] = []
        for idx, row in enumerate(reader):
            if idx >= sample_size:
                break
            records.append(row)
        return records


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

    def parse_records(self, content: str, sample_size: int) -> List[Dict[str, Any]]:
        """Parse dict or list payload into sample_size records."""
        data = json.loads(content)
        if isinstance(data, dict):
            return [data]
        if isinstance(data, list):
            return data[:sample_size]
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
        description="Maximum records to sample when inferring rules",
        required=True,
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=str(DEFAULT_SAMPLE_SIZE),
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
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
        MAX_CATEGORIES,
        REGEX_DERIVATION,
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
                records = handler.parse_records(content_str, self.sample_size)
                stats = self._collect_stats(records)
                rule_yaml = self._build_rules_yaml(stats)
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
                "samples": [],
            }
        )

        for record in records:
            for field, value in record.items():
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

    def _build_rules_yaml(self, stats: Dict[str, Any]) -> str:
        """Convert collected stats into YAML rule definitions."""
        rules = []
        for field, s in stats.items():
            total_seen = s["count"] + s["missing"]
            if total_seen == 0:
                continue
            dqa_type = self._choose_type(s["type_counts"])

            # required/optional
            rules.append(
                {
                    "name": "exists",
                    "feature": field,
                    "specs": {"exists": s["missing"] == 0},
                }
            )

            # datatype rule
            rules.append(
                {
                    "name": "datatype",
                    "feature": field,
                    "specs": {"type": dqa_type},
                }
            )

            # domain for numerics
            if (
                dqa_type in (TypeLabel.INTEGER.value, TypeLabel.FLOAT.value)
                and s["numeric_min"] is not None
                and s["numeric_max"] is not None
            ):
                rules.append(
                    {
                        "name": "domain",
                        "feature": field,
                        "specs": {"min": s["numeric_min"], "max": s["numeric_max"]},
                    }
                )

            # categorical if small distinct
            if s["categories"] and len(s["categories"]) <= self.max_categories:
                rules.append(
                    {
                        "name": "categorical",
                        "feature": field,
                        "specs": {"values": list(s["categories"])},
                    }
                )

            # optional regex
            if self.regex_derivation:
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
