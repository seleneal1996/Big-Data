import os
import shutil

# Ruta de la carpeta general
carpeta_general = "/home/pejelagarta/Desktop/BIG DATA/MapReduce/Gutenberg_Text-master"

# Ruta de la carpeta de destino
carpeta_destino = "/home/pejelagarta/Desktop/BIG DATA/MapReduce/Data"

# Obtener la lista de todos los archivos y subcarpetas en la carpeta general
for root, dirs, files in os.walk(carpeta_general):
    for file in files:
        # Verificar si el archivo tiene extensión .txt
        if file.endswith(".txt"):
            # Ruta completa del archivo
            ruta_archivo = os.path.join(root, file)
            # Ruta de destino para copiar el archivo
            ruta_destino = os.path.join(carpeta_destino, file)
            # Copiar el archivo a la carpeta de destino
            shutil.copy2(ruta_archivo, ruta_destino)
            print("Archivo copiado:", ruta_archivo)

