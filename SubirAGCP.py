import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from os import system

class FormatearParaBigQuery(beam.DoFn):
    def process(self, element):
        import csv
        from io import StringIO

        reader = csv.DictReader(StringIO(element), delimiter=';')
        for fila in reader:
            yield {
                "FechaDeEjecucion": fila.get("FechaDeEjecucion"),
                "Id": fila.get("Id"),
                "Nombre": fila.get("Nombre"),
                "CodigoEstado": fila.get("CodigoEstado"),
                "DescripcionStatus": fila.get("DescripcionStatus"),
                "BusesActivos": fila.get("BusesActivos"),
                "ContadorBusesActivos": fila.get("ContadorBusesActivos"),
                "ServiciosActivos": fila.get("ServiciosActivos"),
                "ServiciosInactivos": fila.get("ServiciosInactivos"),
                "ContadorServiciosActivos": fila.get("ContadorServiciosActivos"),
                "ContadorServiciosInactivos": fila.get("ContadorServiciosInactivos"),
                "ServiciosTotales": fila.get("ServiciosTotales")
            }

# Seteamos el proyecto de GCP
#system("gcloud config set project consideraciones-especificas")
# Habilitamos las APIs necesarias
#system("gcloud services enable dataflow.googleapis.com")
#system("gcloud services enable bigquery.googleapis.com")
# Creamos nuestro bucket (si no existe)
#system("gsutil mb -l us-west1 gs://consideraciones-especificas-realtime || true")
# Copiamos nuestro archivo al bucket
#system("gsutil cp ./Respuestas/* gs://consideraciones-especificas-realtime/")

# Crear la tabla en BigQuery (si no existe)
schema = "FechaDeEjecucion:DATETIME, Id:STRING, Nombre:STRING, CodigoEstado:NUMERIC, DescripcionStatus:STRING,BusesActivos:STRING, ContadorBusesActivos:NUMERIC, ServiciosActivos:STRING, ServiciosInactivos:STRING,ContadorServiciosActivos:NUMERIC, ContadorServiciosInactivos:NUMERIC, ServiciosTotales:NUMERIC"
#crearTabla = f"bq mk --table --schema ./schema.json --description 'Tabla de información de paraderos' consideraciones-especificas:bigdataevaluacion.InformacionParaderos"
#system(crearTabla)

def run(argv=None):
    pipeline_options = PipelineOptions(flags=argv)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'consideraciones-especificas'
    google_cloud_options.job_name = 'realtime-dataflow-job'
    google_cloud_options.staging_location = 'gs://consideraciones-especificas-realtime/staging'
    google_cloud_options.temp_location = 'gs://consideraciones-especificas-realtime/temp'
    google_cloud_options.region = 'us-west1'
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

    p = beam.Pipeline(options=pipeline_options)

    (p
     | 'Read CSV' >> beam.io.ReadFromText('gs://consideraciones-especificas-realtime/InformacionParaderos.csv', skip_header_lines=1)
     | 'Format to BQ Row' >> beam.ParDo(FormatearParaBigQuery())
     | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
         'consideraciones-especificas:bigdataevaluacion.InformacionParaderos',
         schema=schema,
         write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
     ))

    p.run().wait_until_finish()

if __name__ == '__main__':
    run()
