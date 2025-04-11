import fileinput
import glob
import os
import shutil
import time
import re
from itertools import groupby


def copy_raw_files_to_input_folder(n):
    raw_files = glob.glob("files/raw/*.txt")
    os.makedirs("files/input", exist_ok=True)
    for i in range(1, n + 1):
        for file_path in raw_files:
            file_name = os.path.basename(file_path)
            name, ext = os.path.splitext(file_name)
            new_name = f"{name}_{i}{ext}"
            new_path = os.path.join("files/input", new_name)
            shutil.copyfile(file_path, new_path)


def load_input(input_directory):
    input_data = []
    for file_path in glob.glob(f"{input_directory}/*.txt"):
        file_name = os.path.basename(file_path)
        with open(file_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    input_data.append((file_name, line))
    return input_data


def line_preprocessing(sequence):
    result = []
    for file, line in sequence:
        words = re.findall(r"\b\w+\b", line.lower())  # Normaliza a minúsculas
        for word in words:
            result.append((word, 1))
    return result


def mapper(sequence):
    return sequence  # Ya es [(word, 1)]


def shuffle_and_sort(sequence):
    return sorted(sequence, key=lambda x: x[0])


def reducer(sequence):
    result = []
    for key, group in groupby(sequence, key=lambda x: x[0]):
        total = sum(value for _, value in group)
        result.append((key, total))
    return result


def create_ouptput_directory(output_directory):
    if os.path.exists(output_directory):
        shutil.rmtree(output_directory)
    os.makedirs(output_directory)


def save_output(output_directory, sequence):
    output_path = os.path.join(output_directory, "part-00000")
    with open(output_path, "w", encoding="utf-8") as f:
        for key, value in sequence:
            f.write(f"{key}\t{value}\n")


def create_marker(output_directory):
    open(os.path.join(output_directory, "_SUCCESS"), "w").close()


def run_job(input_directory, output_directory):
    create_ouptput_directory(output_directory)
    data = load_input(input_directory)
    preprocessed = line_preprocessing(data)
    mapped = mapper(preprocessed)
    sorted_data = shuffle_and_sort(mapped)
    reduced = reducer(sorted_data)
    save_output(output_directory, reduced)
    create_marker(output_directory)
