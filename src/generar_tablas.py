import csv
import random

# Datos de ejemplo para generar aleatoriamente
nombres = ["Juan", "Ana", "Luis", "Maria", "Pedro", "Lucia", "Carlos", "Sofia", "Diego", "Elena"]
ciudades = ["Madrid", "Barcelona", "Valencia", "Sevilla", "Bilbao", "Zaragoza", "Malaga", "Granada", "Murcia", "Alicante"]

# Generar datos aleatorios
data = [["Nombre", "Edad", "Ciudad"]]
for _ in range(50):
    nombre = random.choice(nombres)
    edad = random.randint(18, 65)
    ciudad = random.choice(ciudades)
    data.append([nombre, edad, ciudad])

# Nombre del archivo CSV
filename = "personas.csv"

# Escribir datos en el archivo CSV
with open(filename, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)