from pathlib import Path
import zipfile
import os
import shutil

def ensure_directory_exists(path):
    path_obj = Path(path)
    path_obj.mkdir(parents=True, exist_ok=True)  # Works like os.makedirs
    print(f"Directory ensured: {path}")

def load_and_extract_zip(zip_src_path:str,extract_path:str,delete_zip:bool=False):
    with zipfile.ZipFile(zip_src_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    if delete_zip:
        os.remove(zip_src_path)

def empty_folder(folder_path):
    """
    Delete all contents of a folder without deleting the folder itself.
    
    Args:
        folder_path (str): Path to the folder to be emptied
    """
    # Verify the folder exists
    if not os.path.isdir(folder_path):
        print(f"The path {folder_path} is not a valid directory.")
        return
    
    # Walk through the folder and remove all files and subdirectories
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")