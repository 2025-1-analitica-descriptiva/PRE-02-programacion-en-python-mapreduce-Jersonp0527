"""Taller evaluable"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os.path
import time
from itertools import groupby


#
# Escriba la funcion que  genere n copias de los archivos de texto en la
# carpeta files/raw en la carpeta files/input. El nombre de los archivos
# generados debe ser el mismo que el de los archivos originales, pero con
# un sufijo que indique el número de copia. Por ejemplo, si el archivo
# original se llama text0.txt, el archivo generado se llamará text0_1.txt,
# text0_2.txt, etc.
#
def copy_raw_files_to_input_folder(n):
    """Funcion copy_files"""
    # Crear directorio input si no existe
    if not os.path.exists("files/input"):
        os.makedirs("files/input")

    # Obtener lista de archivos en raw
    raw_files = glob.glob("files/raw/*.txt")

    # Para cada archivo en raw
    for raw_file in raw_files:
        # Obtener el nombre base del archivo
        base_name = os.path.basename(raw_file)
        name_without_ext = os.path.splitext(base_name)[0]

        # Crear n copias
        for i in range(1, n + 1):
            # Crear el nombre del archivo de salida
            output_name = f"{name_without_ext}_{i}.txt"
            output_path = os.path.join("files/input", output_name)

            # Copiar el archivo
            with open(raw_file, "r", encoding="utf-8") as src:
                with open(output_path, "w", encoding="utf-8") as dst:
                    dst.write(src.read())


#
# Escriba la función load_input que recive como parámetro un folder y retorna
# una lista de tuplas donde el primer elemento de cada tupla es el nombre del
# archivo y el segundo es una línea del archivo. La función convierte a tuplas
# todas las lineas de cada uno de los archivos. La función es genérica y debe
# leer todos los archivos de folder entregado como parámetro.
#
# Por ejemplo:
#   [
#     ('text0'.txt', 'Analytics is the discovery, inter ...'),
#     ('text0'.txt', 'in data. Especially valuable in ar...').
#     ...
#     ('text2.txt'. 'hypotheses.')
#   ]
#
def load_input(input_directory):
    """Funcion load_input"""
    lines = []
    # Obtener todos los archivos .txt en el directorio
    input_files = glob.glob(os.path.join(input_directory, "*.txt"))

    # Para cada archivo
    for input_file in input_files:
        # Obtener el nombre del archivo
        file_name = os.path.basename(input_file)

        # Leer cada línea del archivo
        with open(input_file, "r", encoding="utf-8") as file:
            for line in file:
                # Agregar tupla (nombre_archivo, línea)
                lines.append((file_name, line.strip()))

    return lines


#
# Escriba la función line_preprocessing que recibe una lista de tuplas de la
# función anterior y retorna una lista de tuplas (clave, valor). Esta función
# realiza el preprocesamiento de las líneas de texto,
#
def line_preprocessing(sequence):
    """Line Preprocessing"""
    processed = []
    for file_name, line in sequence:
        # Convertir a minúsculas
        line = line.lower()
        # Eliminar caracteres especiales y números
        line = "".join(c for c in line if c.isalpha() or c.isspace())
        # Dividir en palabras
        words = line.split()
        # Agregar cada palabra con el nombre del archivo
        for word in words:
            if word:  # Ignorar palabras vacías
                processed.append((word, 1))
    return processed


#
# Escriba una función llamada maper que recibe una lista de tuplas de la
# función anterior y retorna una lista de tuplas (clave, valor). En este caso,
# la clave es cada palabra y el valor es 1, puesto que se está realizando un
# conteo.
#
#   [
#     ('Analytics', 1),
#     ('is', 1),
#     ...
#   ]
#
def mapper(sequence):
    """Mapper"""
    mapped = []
    for word, count in sequence:
        mapped.append((word, 1))
    return mapped


#
# Escriba la función shuffle_and_sort que recibe la lista de tuplas entregada
# por el mapper, y retorna una lista con el mismo contenido ordenado por la
# clave.
#
#   [
#     ('Analytics', 1),
#     ('Analytics', 1),
#     ...
#   ]
#
def shuffle_and_sort(sequence):
    """Shuffle and Sort"""
    # Ordenar por la clave (palabra)
    return sorted(sequence, key=lambda x: x[0])


#
# Escriba la función reducer, la cual recibe el resultado de shuffle_and_sort y
# reduce los valores asociados a cada clave sumandolos. Como resultado, por
# ejemplo, la reducción indica cuantas veces aparece la palabra analytics en el
# texto.
#
def reducer(sequence):
    """Reducer"""
    reduced = []
    # Agrupar por palabra
    for key, group in groupby(sequence, key=lambda x: x[0]):
        # Sumar los valores
        total = sum(value for _, value in group)
        reduced.append((key, total))
    return reduced


#
# Escriba la función create_ouptput_directory que recibe un nombre de
# directorio y lo crea. Si el directorio existe, lo borra
#
def create_ouptput_directory(output_directory):
    """Create Output Directory"""
    # Si el directorio existe, borrarlo
    if os.path.exists(output_directory):
        for file in os.listdir(output_directory):
            os.remove(os.path.join(output_directory, file))
        os.rmdir(output_directory)

    # Crear el directorio
    os.makedirs(output_directory)


#
# Escriba la función save_output, la cual almacena en un archivo de texto
# llamado part-00000 el resultado del reducer. El archivo debe ser guardado en
# el directorio entregado como parámetro, y que se creo en el paso anterior.
# Adicionalmente, el archivo debe contener una tupla por línea, donde el primer
# elemento es la clave y el segundo el valor. Los elementos de la tupla están
# separados por un tabulador.
#
def save_output(output_directory, sequence):
    """Save Output"""
    output_file = os.path.join(output_directory, "part-00000")
    with open(output_file, "w", encoding="utf-8") as f:
        for key, value in sequence:
            f.write(f"{key}\t{value}\n")


#
# La siguiente función crea un archivo llamado _SUCCESS en el directorio
# entregado como parámetro.
#
def create_marker(output_directory):
    """Create Marker"""
    marker_file = os.path.join(output_directory, "_SUCCESS")
    with open(marker_file, "w", encoding="utf-8") as f:
        f.write("")


#
# Escriba la función job, la cual orquesta las funciones anteriores.
#
def run_job(input_directory, output_directory):
    """Job"""
    # Cargar los datos
    lines = load_input(input_directory)

    # Preprocesar las líneas
    processed = line_preprocessing(lines)

    # Mapear
    mapped = mapper(processed)

    # Shuffle and sort
    sorted_data = shuffle_and_sort(mapped)

    # Reducir
    reduced = reducer(sorted_data)

    # Crear directorio de salida
    create_ouptput_directory(output_directory)

    # Guardar resultados
    save_output(output_directory, reduced)

    # Crear marcador
    create_marker(output_directory)


if __name__ == "__main__":

    copy_raw_files_to_input_folder(n=1000)

    start_time = time.time()

    run_job(
        "files/input",
        "files/output",
    )

    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")