"""Configuration loading and validation for the RDC-BigQuery bridge."""

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class SchemaField:
    """BigQuery schema field definition."""
    name: str
    type: str
    mode: str
    description: str = ""


@dataclass
class TableConfig:
    """Configuration for a single BigQuery table."""
    name: str
    description: str
    partitioning_field: str
    partitioning_type: str
    clustering_fields: List[str]
    schema: List[SchemaField]
    field_mappings: Dict[str, str]
    field_types: Dict[str, str]


@dataclass
class BigQueryConfig:
    """BigQuery-specific configuration."""
    project_id: str
    dataset_id: str
    location: str
    tables: Dict[str, TableConfig]


@dataclass
class GCPConfig:
    """Google Cloud Platform configuration."""
    project_id: str
    dataset_id: str
    credentials_file: str


@dataclass
class RedisConfig:
    """Redis configuration."""
    host: str
    port: int
    username: str | None
    password: str | None


@dataclass
class RoutingRule:
    """Configuration for a single routing rule."""
    name: str
    redis_source_type: str  # "key" or "channel"
    redis_pattern: str
    target_table: str
    id_parser_regex: Optional[str] = None
    value_is_object: bool = False
    column_name: Optional[str] = None
    compiled_regex: Optional[re.Pattern] = None


@dataclass
class LoaderConfig:
    """BigQuery loader configuration."""
    batch_size_rows: int
    batch_size_bytes: int
    commit_interval_seconds: float


@dataclass
class PipelineConfig:
    """Pipeline configuration."""
    routing_rules: List[RoutingRule]


@dataclass
class Config:
    """Main configuration object."""
    gcp: GCPConfig
    redis: RedisConfig
    pipeline: PipelineConfig
    loader: LoaderConfig
    bigquery: Optional[BigQueryConfig] = None


class ConfigValidationError(Exception):
    """Raised when configuration validation fails."""
    pass


def load_config(config_path: str) -> Config:
    """Load and validate configuration from YAML file."""
    config_file = Path(config_path)

    if not config_file.exists():
        raise ConfigValidationError(
            f"Configuration file not found: {config_path}")

    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            raw_config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigValidationError(f"Invalid YAML in config file: {e}")

    return _validate_and_parse_config(raw_config)


def _validate_and_parse_config(raw_config: Dict[str, Any]) -> Config:
    """Validate and parse raw configuration dictionary."""
    try:
        # Validate GCP config
        gcp_config = _validate_gcp_config(raw_config.get('gcp', {}))

        # Validate Redis config
        redis_config = _validate_redis_config(raw_config.get('redis', {}))

        # Validate pipeline config
        pipeline_config = _validate_pipeline_config(
            raw_config.get('pipeline', {}))

        # Validate loader config
        loader_config = _validate_loader_config(raw_config.get('loader', {}))

        # Validate BigQuery config (optional for backwards compatibility)
        bigquery_config = None
        if 'bigquery' in raw_config:
            bigquery_config = _validate_bigquery_config(raw_config.get('bigquery', {}))

        return Config(
            gcp=gcp_config,
            redis=redis_config,
            pipeline=pipeline_config,
            loader=loader_config,
            bigquery=bigquery_config
        )

    except (KeyError, TypeError, ValueError) as e:
        raise ConfigValidationError(f"Configuration validation error: {e}")


def _validate_gcp_config(gcp_config: Dict[str, Any]) -> GCPConfig:
    """Validate GCP configuration section."""
    required_fields = ['project_id', 'dataset_id', 'credentials_file']

    for field in required_fields:
        if field not in gcp_config:
            raise ConfigValidationError(
                f"Missing required GCP config field: {field}")

    credentials_file = gcp_config['credentials_file']

    # Allow environment variable substitution
    if credentials_file and credentials_file.startswith('$'):
        env_var = credentials_file[1:]
        credentials_file = os.getenv(env_var)
        if not credentials_file:
            raise ConfigValidationError(
                f"Environment variable {env_var} not set")

    # Validate credentials file exists
    if credentials_file and not Path(credentials_file).exists():
        raise ConfigValidationError(
            f"GCP credentials file not found: {credentials_file}")

    return GCPConfig(
        project_id=gcp_config['project_id'],
        dataset_id=gcp_config['dataset_id'],
        credentials_file=credentials_file
    )


def _validate_redis_config(redis_config: Dict[str, Any]) -> RedisConfig:
    """Validate Redis configuration section."""
    return RedisConfig(host=redis_config['host'], port=redis_config['port'], username=redis_config.get('username'), password=redis_config.get('password'))


def _validate_pipeline_config(pipeline_config: Dict[str, Any]) -> PipelineConfig:
    """Validate pipeline configuration section."""
    if 'routing_rules' not in pipeline_config:
        raise ConfigValidationError(
            "Missing required pipeline config field: routing_rules")

    routing_rules = []

    for i, rule_config in enumerate(pipeline_config['routing_rules']):
        try:
            routing_rule = _validate_routing_rule(rule_config)
            routing_rules.append(routing_rule)
        except ConfigValidationError as e:
            raise ConfigValidationError(f"Error in routing rule {i}: {e}")

    return PipelineConfig(routing_rules=routing_rules)


def _validate_routing_rule(rule_config: Dict[str, Any]) -> RoutingRule:
    """Validate a single routing rule."""
    required_fields = ['name', 'redis_source_type',
                       'redis_pattern', 'target_table']

    for field in required_fields:
        if field not in rule_config:
            raise ConfigValidationError(
                f"Missing required routing rule field: {field}")

    # Validate redis_source_type
    if rule_config['redis_source_type'] not in ['key', 'channel']:
        raise ConfigValidationError(
            f"Invalid redis_source_type: {rule_config['redis_source_type']}. "
            "Must be 'key' or 'channel'"
        )

    # Validate target table
    valid_tables = ['empatica', 'blueiot', 'global_state_events']
    if rule_config['target_table'] not in valid_tables:
        raise ConfigValidationError(
            f"Invalid target_table: {rule_config['target_table']}. "
            f"Must be one of: {valid_tables}"
        )

    # id_parser_regex is required for empatica/blueiot but optional for global_state_events
    id_parser_regex = rule_config.get('id_parser_regex')
    compiled_regex = None

    if rule_config['target_table'] in ['empatica', 'blueiot']:
        if not id_parser_regex:
            raise ConfigValidationError(
                f"id_parser_regex is required for {rule_config['target_table']} table"
            )
        try:
            compiled_regex = re.compile(id_parser_regex)
        except re.error as e:
            raise ConfigValidationError(f"Invalid regex pattern: {e}")
    elif id_parser_regex:
        # If provided for global_state_events, still validate it
        try:
            compiled_regex = re.compile(id_parser_regex)
        except re.error as e:
            raise ConfigValidationError(f"Invalid regex pattern: {e}")

    return RoutingRule(
        name=rule_config['name'],
        redis_source_type=rule_config['redis_source_type'],
        redis_pattern=rule_config['redis_pattern'],
        target_table=rule_config['target_table'],
        id_parser_regex=id_parser_regex,
        value_is_object=rule_config.get('value_is_object', False),
        column_name=rule_config.get('column_name'),
        compiled_regex=compiled_regex
    )


def _validate_loader_config(loader_config: Dict[str, Any]) -> LoaderConfig:
    """Validate loader configuration section."""
    required_fields = ['batch_size_rows',
                       'batch_size_bytes', 'commit_interval_seconds']

    for field in required_fields:
        if field not in loader_config:
            raise ConfigValidationError(
                f"Missing required loader config field: {field}")

    batch_size_rows = loader_config['batch_size_rows']
    batch_size_bytes = loader_config['batch_size_bytes']
    commit_interval = loader_config['commit_interval_seconds']

    if not isinstance(batch_size_rows, int) or batch_size_rows <= 0:
        raise ConfigValidationError(
            "batch_size_rows must be a positive integer")

    if not isinstance(batch_size_bytes, int) or batch_size_bytes <= 0:
        raise ConfigValidationError(
            "batch_size_bytes must be a positive integer")

    if not isinstance(commit_interval, (int, float)) or commit_interval <= 0:
        raise ConfigValidationError(
            "commit_interval_seconds must be a positive number")

    return LoaderConfig(
        batch_size_rows=batch_size_rows,
        batch_size_bytes=batch_size_bytes,
        commit_interval_seconds=float(commit_interval)
    )


def _validate_bigquery_config(bq_config: Dict[str, Any]) -> BigQueryConfig:
    """Validate BigQuery configuration section."""
    required_fields = ['project_id', 'dataset_id', 'location', 'tables']
    
    for field in required_fields:
        if field not in bq_config:
            raise ConfigValidationError(
                f"Missing required BigQuery config field: {field}")
    
    tables = {}
    for table_name, table_config in bq_config['tables'].items():
        tables[table_name] = _validate_table_config(table_name, table_config)
    
    return BigQueryConfig(
        project_id=bq_config['project_id'],
        dataset_id=bq_config['dataset_id'],
        location=bq_config['location'],
        tables=tables
    )


def _validate_table_config(table_name: str, table_config: Dict[str, Any]) -> TableConfig:
    """Validate a single table configuration."""
    required_fields = ['name', 'description', 'partitioning_field', 
                      'partitioning_type', 'clustering_fields', 'schema']
    
    for field in required_fields:
        if field not in table_config:
            raise ConfigValidationError(
                f"Missing required field '{field}' in table config for {table_name}")
    
    # Parse schema fields
    schema_fields = []
    for field_config in table_config['schema']:
        schema_fields.append(SchemaField(
            name=field_config['name'],
            type=field_config['type'],
            mode=field_config['mode'],
            description=field_config.get('description', '')
        ))
    
    # Field mappings are optional
    field_mappings = table_config.get('field_mappings', {})
    
    # Field types are optional
    field_types = table_config.get('field_types', {})
    
    return TableConfig(
        name=table_config['name'],
        description=table_config['description'],
        partitioning_field=table_config['partitioning_field'],
        partitioning_type=table_config['partitioning_type'],
        clustering_fields=table_config['clustering_fields'],
        schema=schema_fields,
        field_mappings=field_mappings,
        field_types=field_types
    )


def get_default_config_path() -> str:
    """Get the default configuration file path."""
    return os.getenv('RDC_BRIDGE_CONFIG', 'config/config.yaml')