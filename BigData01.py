import time
import concurrent.futures
import os
import matplotlib.pyplot as plt
import numpy as np

# Función Map: realiza la tarea de contar las ocurrencias de las palabras en un fragmento de texto y 
# devuelve un diccionario que contiene las palabras y sus respectivos recuentos en ese fragmento.
def mapper(text):
    word_counts = {}
    words = text.split()
    for word in words:
        if word in word_counts:
            word_counts[word] += 1
        else:
            word_counts[word] = 1
    return word_counts

# Función Reduce: toma una lista de resultados parciales de los mappers, combina las ocurrencias 
# de las palabras y devuelve un diccionario que contiene las palabras y sus recuentos combinados.

def reducer(results):
    word_counts = {}
    for result in results:
        for word, count in result.items():
            if word in word_counts:
                word_counts[word] += count
            else:
                word_counts[word] = count
    return word_counts

def main():
    input_path = '/home/pejelagarta/Desktop/BIG DATA/MapReduce/Data'
    for num_threads in [1,2,3,4]:
        for data_percentage in [0.10,0.25,0.50,1]:
            output_path = f'/home/pejelagarta/Desktop/BIG DATA/MapReduce/Results/result_{num_threads}_threads.txt'

            # Obtener la lista de archivos .txt en el directorio de entrada
            txt_files = [file for file in os.listdir(input_path) if file.endswith(".txt")]

            start_time = time.time()

            # Crear un ThreadPoolExecutor con el número de hilos especificado
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                # Procesar cada archivo .txt en paralelo
                mapper_results = executor.map(process_file, txt_files, [data_percentage] * len(txt_files))

                # Combinar los resultados parciales de los mappers utilizando la función reducer
                word_counts = reducer(mapper_results)

            end_time = time.time()
            processing_time = end_time - start_time

            # Guardar los resultados en un archivo de salida
            with open(output_path, 'w') as file:
                for word, count in word_counts.items():
                    file.write(f'{word}\t{count}\n')

            # Imprimir el tiempo de procesamiento
            print(f'Tiempo de procesamiento con {num_threads} hilos: {processing_time} segundos con un {data_percentage*100} % de la data')

def process_file(file_name, data_percentage):
    input_path = '/home/pejelagarta/Desktop/BIG DATA/MapReduce/Data'
    file_path = os.path.join(input_path, file_name)
    with open(file_path, 'r') as file:
        data = file.read()

    # Obtener una fracción del texto para procesar
    data_size = len(data)
    data_to_process = data[:int(data_size * data_percentage)]

    # Dividir los datos en fragmentos para los mappers
    num_threads = 1  # Se utiliza un solo hilo dentro de cada proceso
    fragment_size = len(data_to_process) // num_threads
    fragments = [data_to_process[i:i+fragment_size] for i in range(0, len(data_to_process), fragment_size)]

    # Aplicar la función mapper a cada fragmento de texto
    mapper_results = map(mapper, fragments)

    return reducer(mapper_results)
    
def plot_execution_time():
    input_path = '/home/pejelagarta/Desktop/BIG DATA/MapReduce/Results'
    num_threads = [1, 2, 3, 4]
    data_percentages = [0.10,0.25, 0.50, 1]
    execution_times = []

    for percentage in data_percentages:
        thread_times = []
        for thread in num_threads:
            file_path = os.path.join(input_path, f'result_{thread}_threads.txt')
            with open(file_path, 'r') as file:
                lines = file.readlines()
                time = float(lines[-1].split(':')[1].strip().split(' ')[0])
                thread_times.append(time)
        execution_times.append(thread_times)

    # Convert lists to NumPy arrays
    num_threads = np.array(num_threads)
    execution_times = np.array(execution_times)

    # Plotting
    fig, ax = plt.subplots()
    x_ticks = np.arange(len(data_percentages))  # Use a numerical range instead of percentage labels
    for i in range(len(num_threads)):
        ax.plot(x_ticks, execution_times[:, i], label=f'{num_threads[i]} threads')
    ax.set_xticks(x_ticks)
    ax.set_xticklabels([f'{p*100}%' for p in data_percentages])
    ax.set_xlabel('Percentage of Data')
    ax.set_ylabel('Execution Time (seconds)')
    ax.set_title('Execution Time vs Percentage of Data')
    ax.legend()
    plt.show()
    
if __name__ == '__main__':
    main()
    #plot_execution_time()
