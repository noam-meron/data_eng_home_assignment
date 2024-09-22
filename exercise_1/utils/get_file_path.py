import os


def get_file_path(file_name: str, folder_name: str) -> str:

    src_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(src_dir)
    file_path = os.path.join(project_root, folder_name, file_name)

    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File '{file_name}' not found at {file_path}")

    return file_path
