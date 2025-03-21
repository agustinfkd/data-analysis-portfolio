# Proyecto de Limpieza y Análisis Exploratorio de Datos en SQL

## Descripción del Proyecto:
Este proyecto se enfoca en la limpieza y análisis exploratorio de un dataset sobre despidos masivos en empresas a nivel mundial. Se utiliza SQL para transformar y explorar los datos, con el objetivo de identificar patrones y tendencias clave.

## Dataset
Fuente: https://www.kaggle.com/datasets/swaptr/layoffs-2022
Archivos principales:
- layoffs.csv: Contiene los datos originales sin procesar.
- layoffs_staging2.csv: Versión intermedia con transformaciones aplicadas.

## Tecnologías Utilizadas:
- MySQL Workbench

## Parte 1: Limpieza de Datos (data cleaning project.sql)
Se creó una tabla de trabajo (layoffs_staging) para realizar las transformaciones sin afectar los datos originales.

## Pasos de Limpieza:
- Creación de una tabla de staging: Copia de los datos originales en layoffs_staging.
- Eliminación de duplicados: Se identificaron y eliminaron registros duplicados.
- Corrección de datos nulos: Se imputaron valores en columnas clave como company e industry.
- Corrección de formatos: Se aseguraron formatos consistentes en fechas y valores numéricos.
- Normalización de valores: Se homogeneizaron nombres de empresas y sectores.

## Parte 2: Análisis Exploratorio (exploratory data analysis project.sql)
Se realizaron consultas para entender mejor el dataset y extraer información relevante.
