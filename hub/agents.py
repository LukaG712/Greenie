from pathlib import Path
from typing import Any, Dict, List, Optional

from hub.config import (
    add_agent,
    delete_agent,
    get_agent_config,
    get_config_path,
    load_config,
    save_config,
    update_agent,
    validate_name,
)


def list_agents() -> List[str]:
    config = load_config()
    return list(config.get("agents", {}).keys())


def get_agent(name: str) -> Dict[str, Any]:
    config = load_config()
    return get_agent_config(config, name)


def create_agent(
    name: str,
    runner: str,
    project_dir: str,
    title: str = "",
    description: str = "",
    skills: Optional[List[str]] = None,
    guidelines: str = "",
    permissions: Optional[Dict[str, bool]] = None,
) -> Dict[str, Any]:
    if not validate_name(name):
        raise ValueError(f"Invalid agent name: {name}")
    # Use load_config_locked to prevent race conditions with concurrent modifications
    from hub.config import load_config_locked
    config = load_config_locked()
    agent_data = {
        "title": title,
        "description": description,
        "runner": runner,
        "project_dir": project_dir,
        "skills": skills or [],
        "guidelines": guidelines,
    }
    if permissions is not None:
        agent_data["permissions"] = permissions
    config = add_agent(config, name, agent_data)
    save_config(config)
    return agent_data


def update_agent_data(name: str, updates: Dict[str, Any]) -> Dict[str, Any]:
    # Use load_config_locked to prevent race conditions with concurrent modifications
    from hub.config import load_config_locked
    config = load_config_locked()
    config = update_agent(config, name, updates)
    save_config(config)
    return get_agent_config(config, name)


def delete_agent_by_name(name: str) -> None:
    # Use load_config_locked to prevent race conditions with concurrent modifications
    from hub.config import load_config_locked
    config = load_config_locked()
    config = delete_agent(config, name)
    save_config(config)


def agent_exists(name: str) -> bool:
    try:
        get_agent(name)
        return True
    except KeyError:
        return False
