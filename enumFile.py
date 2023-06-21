import os

def enumerate_and_rename_files(directory):
    file_list = os.listdir(directory)
    txt_files = [file for file in file_list if file.endswith(".txt")]
    txt_files.sort()  # Ordenar los archivos alfabéticamente

    for i, file_name in enumerate(txt_files, 1):
        base_name = os.path.splitext(file_name)[0]
        new_name = f"file{i}.txt"
        new_path = os.path.join(directory, new_name)
        old_path = os.path.join(directory, file_name)
        os.rename(old_path, new_path)
        print(f"Renombrado: {file_name} -> {new_name}")

directory = "/home/pejelagarta/Desktop/BIG DATA/MapReduce/Data"  # Ruta al directorio que contiene los archivos .txt
enumerate_and_rename_files(directory)

