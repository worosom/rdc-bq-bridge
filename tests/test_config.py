"""Test configuration loading and validation."""

import pytest
import tempfile
import os
from pathlib import Path

from src.config import Config, ConfigValidationError, load_config


def test_load_valid_config():
    """Test loading a valid configuration."""
    config_content = """
gcp:
  project_id: "test-project"
  dataset_id: "test_dataset"
  credentials_file: "/tmp/test-creds.json"

redis:
  host: "localhost"
  port: 6379
  user: "default"
  password: "password"

pipeline:
  routing_rules:
    - name: "TestRule"
      redis_source_type: "key"
      redis_pattern: "test:*"
      target_table: "empatica"
      id_parser_regex: "test:(?P<device_id>[^:]+)"
      value_is_object: false

loader:
  batch_size_rows: 100
  batch_size_bytes: 1000000
  commit_interval_seconds: 1.0
"""
    
    # Create temporary credentials file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as creds_file:
        creds_file.write('{"type": "service_account"}')
        creds_path = creds_file.name
    
    try:
        # Create temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as config_file:
            # Update config to use actual credentials file
            updated_config = config_content.replace('/tmp/test-creds.json', creds_path)
            config_file.write(updated_config)
            config_path = config_file.name
        
        # Load and validate config
        config = load_config(config_path)
        
        # Assertions
        assert config.gcp.project_id == "test-project"
        assert config.gcp.dataset_id == "test_dataset"
        assert config.redis.host == "localhost"
        assert config.redis.port == 6379
        assert len(config.pipeline.routing_rules) == 1
        assert config.loader.batch_size_rows == 100
        
        # Clean up
        os.unlink(config_path)
        
    finally:
        if os.path.exists(creds_path):
            os.unlink(creds_path)


def test_invalid_config_missing_section():
    """Test that missing required sections raise validation errors."""
    config_content = """
gcp:
  project_id: "test-project"
# Missing other required sections
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as config_file:
        config_file.write(config_content)
        config_path = config_file.name
    
    try:
        with pytest.raises(ConfigValidationError):
            load_config(config_path)
    finally:
        os.unlink(config_path)


def test_invalid_regex_pattern():
    """Test that invalid regex patterns raise validation errors."""
    config_content = """
gcp:
  project_id: "test-project"
  dataset_id: "test_dataset"
  credentials_file: "/tmp/test-creds.json"

redis:
  host: "localhost"
  port: 6379

pipeline:
  routing_rules:
    - name: "TestRule"
      redis_source_type: "key"
      redis_pattern: "test:*"
      target_table: "empatica"
      id_parser_regex: "[invalid regex("  # Invalid regex
      value_is_object: false

loader:
  batch_size_rows: 100
  batch_size_bytes: 1000000
  commit_interval_seconds: 1.0
"""
    
    # Create temporary credentials file to satisfy GCP check
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as creds_file:
        creds_file.write('{"type": "service_account"}')
        creds_path = creds_file.name

    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as config_file:
            # Update config to use actual credentials file
            updated_config = config_content.replace('/tmp/test-creds.json', creds_path)
            config_file.write(updated_config)
            config_path = config_file.name
    
        with pytest.raises(ConfigValidationError, match="Invalid regex pattern"):
            load_config(config_path)
            
    finally:
        if os.path.exists(config_path):
            os.unlink(config_path)
        if os.path.exists(creds_path):
            os.unlink(creds_path)
