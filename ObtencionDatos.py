import requests
import json
from time import sleep
from datetime import datetime
from os import system
from os import path
from os import makedirs
import csv

# Declaración de constantes
UrlObtenerInfoParaderos = "https://api.xor.cl/red/bus-stop/"
segundosDeEspera = 5
mensajeEspera = f"Esperando {segundosDeEspera} segundos para no saturar el servidor..."
listaParaderos = [ # seleccionamos 10 paraderos al azar debido a que no tenemos una api que nos permita obtener todos los paraderos
    "PI148",
    "PI419",
    "PI230",
    "PA95",
    "PJ1731",
    "PF101",
    "PB1921",
    "PJ108",
    "PA279",
    "PD10"
]

def GetRespuestaApi(url: str, paradero: str) -> dict:
    """
    Realiza una petición GET a una API y retorna la respuesta en formato JSON

    ### PARAMETROS:
    - url (str): URL de la API a consultar
    - paradero (str): Código del paradero
    ### RETORNO:
    - dict: Respuesta de la API en formato JSON
    """
    urlCompleta = url + paradero
    try:
        respuesta = requests.get(urlCompleta).json()
    except Exception as e:
        # En caso de error, respuesta sera el mensaje de error en formato json
        respuesta = {"error": str(e)}
    return respuesta

def FiltrarRespuesta(respuesta: dict) -> dict:
    """
    Busca tomar la respuesta de la api y filtrarla para obtener solo la información necesaria.
    Ademas, aplana la respuesta para que sea más fácil de leer por BigQuery en gcp.

    ### PARAMETROS:
    - respuesta (dict): Respuesta de la API en formato JSON
    ### RETORNO:
    - dict: Información filtrada de la respuesta en formato JSON
    """
    respuestaFiltrada = {}
    serviciosActivos = ""
    serviciosNoActivos = ""
    patentesDeBusesActivos = ""
    contadorBusesActivos = 0
    contadorServiciosActivos = 0
    contadorServiciosNoActivos = 0

    if respuesta.get("error"):
        return respuesta

    respuestaFiltrada["FechaDeEjecucion"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    respuestaFiltrada["Id"] = respuesta["id"]
    respuestaFiltrada["Nombre"] = respuesta["name"]
    respuestaFiltrada["CodigoEstado"] = respuesta["status_code"]
    respuestaFiltrada["DescripcionStatus"] = respuesta["status_description"]

    for servicio in respuesta["services"]:
        if servicio["valid"]:
            serviciosActivos += servicio["id"] + ", "
            contadorServiciosActivos += 1
            for bus in servicio["buses"]:
                patentesDeBusesActivos += bus["id"] + ", "
                contadorBusesActivos += 1
        else:
            serviciosNoActivos += servicio["id"] + ", "
            contadorServiciosNoActivos += 1

    respuestaFiltrada["BusesActivos"] = patentesDeBusesActivos[:-2]
    respuestaFiltrada["ContadorBusesActivos"] = contadorBusesActivos
    respuestaFiltrada["ServiciosActivos"] = serviciosActivos[:-2]
    respuestaFiltrada["ServiciosInactivos"] = serviciosNoActivos[:-2]
    respuestaFiltrada["ContadorServiciosActivos"] = contadorServiciosActivos
    respuestaFiltrada["ContadorServiciosInactivos"] = contadorServiciosNoActivos
    respuestaFiltrada["ServiciosTotales"] = contadorServiciosActivos + contadorServiciosNoActivos
    return respuestaFiltrada

def GuardarRespuestaEnArchivo(respuesta: dict) -> None:
    """
    Guarda la respuesta de la API en un archivo JSON.
    El archivo sera guardado en la carpeta "Respuestas" con el nombre del paradero en formato json y csv.
    """
    nombreArchivoJson = f"{respuesta['Id']}.json"
    nombreArchivoCsv = f"InformacionParaderos.csv"

    if not path.exists("Respuestas"):
        makedirs("Respuestas")

    with open(f"Respuestas/{nombreArchivoJson}", "w") as archivoJson:
        json.dump(respuesta, archivoJson, indent=4)

    # Generar un solo archivo csv con la información de todos los paraderos con separador ";" en formato utf8 en la ruta Respuestas/InformacionParaderos.csv
    with open(f"Respuestas/{nombreArchivoCsv}", "a", newline="", encoding="utf-8") as archivoCsv:
        escritorCsv = csv.DictWriter(archivoCsv, fieldnames=respuesta.keys(), delimiter=";")
        if archivoCsv.tell() == 0:
            escritorCsv.writeheader()
        escritorCsv.writerow(respuesta)


def main():
    for paradero in listaParaderos:
        try:
            print(f"Obteniendo información de paradero {paradero}...")
            respuesta = GetRespuestaApi(UrlObtenerInfoParaderos, paradero)
            respuestaFiltrada = FiltrarRespuesta(respuesta)
            GuardarRespuestaEnArchivo(respuestaFiltrada)
            print(f"Guardado archivo {paradero}.json")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {mensajeEspera}")
            sleep(segundosDeEspera)
        except Exception as e:
            print(f"Error al obtener información de paradero {paradero}: {str(e)}")
            continue

if __name__ == "__main__":
    main()