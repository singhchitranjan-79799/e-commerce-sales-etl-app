import os
from dotenv import load_dotenv


def load_environment(env_file_name: str = "credentials.env") -> None:
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    env_path = os.path.join(project_root, "Env_variable", env_file_name)
    load_dotenv(env_path)
    return None


__all__ = ["load_environment"]
