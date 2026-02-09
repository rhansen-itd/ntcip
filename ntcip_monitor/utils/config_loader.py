"""
Configuration Loader with Hot-Reload Support
"""

import json
import os
from typing import Dict, Any, Optional
from datetime import datetime


class ConfigLoader:
    """
    Load and monitor configuration file for changes.
    
    Supports hot-reload by checking file modification time.
    """
    
    def __init__(self, config_path='config.json'):
        """
        Initialize config loader.
        
        Args:
            config_path: Path to JSON configuration file
        """
        self.config_path = config_path
        self._config: Dict[str, Any] = {}
        self._last_modified: Optional[float] = None
        self._load()
    
    def _load(self):
        """Load configuration from file."""
        try:
            with open(self.config_path, 'r') as f:
                self._config = json.load(f)
            
            # Update last modified time
            self._last_modified = os.path.getmtime(self.config_path)
            
            print(f"Configuration loaded from {self.config_path}")
            
        except FileNotFoundError:
            print(f"Warning: Config file {self.config_path} not found. Using defaults.")
            self._config = self._get_default_config()
            self._create_default_config_file()
        except json.JSONDecodeError as e:
            print(f"Error parsing {self.config_path}: {e}")
            self._config = self._get_default_config()
    
    def check_for_updates(self) -> bool:
        """
        Check if config file has been modified and reload if needed.
        
        Returns:
            bool: True if config was reloaded, False otherwise
        """
        try:
            current_mtime = os.path.getmtime(self.config_path)
            
            if current_mtime != self._last_modified:
                print(f"Config file modified, reloading...")
                self._load()
                return True
                
        except FileNotFoundError:
            pass
        
        return False
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value.
        
        Args:
            key: Configuration key (supports dot notation, e.g., 'controller.ip')
            default: Default value if key not found
        
        Returns:
            Configuration value or default
        """
        # Support dot notation
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_all(self) -> Dict[str, Any]:
        """Get entire configuration dictionary."""
        return self._config.copy()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            "controller": {
                "ip": "10.37.2.68",
                "port": 501,
                "community": "administrator",
                "timeout": 2,
                "retries": 2
            },
            "monitors": {
                "phases": {
                    "enabled": True,
                    "poll_interval": 0.25,
                    "monitor_1_8": True,
                    "monitor_9_16": False,
                    "monitor_overlaps": False
                },
                "detectors": {
                    "enabled": False,
                    "poll_interval": 0.1,
                    "detector_range": [1, 65]
                },
                "outputs": {
                    "enabled": False,
                    "poll_interval": 0.25,
                    "output_range": [1, 17]
                }
            },
            "logging": {
                "enabled": True,
                "log_file": "ntcip_monitor.log",
                "log_changes_only": True
            },
            "web_ui": {
                "enabled": True,
                "host": "0.0.0.0",
                "port": 5000
            }
        }
    
    def _create_default_config_file(self):
        """Create default config file if it doesn't exist."""
        try:
            with open(self.config_path, 'w') as f:
                json.dump(self._config, f, indent=2)
            print(f"Created default config file: {self.config_path}")
        except Exception as e:
            print(f"Could not create config file: {e}")
