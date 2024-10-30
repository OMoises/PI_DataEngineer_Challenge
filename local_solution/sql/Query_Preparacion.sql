--######################################################################--
--LIMPIEZA DE BD
--######################################################################--

--Primero vemos la cantidad de datos unicos segun la llave comentada
SELECT COUNT(1) AS cant,COUNT(DISTINCT CONCAT(id, muestra, resultado)) AS cant_unica FROM [Testing_ETL].[dbo].[Unificado];
--cant	cant_unica
--3873	1805

--Limpiamos los duplicados basandonos en la logica de documento tomando la fecha_copia mas reciente
WITH base_duplicada AS (
	SELECT
		a.*,
		ROW_NUMBER()OVER(PARTITION BY id, muestra, resultado ORDER BY fecha_copia DESC) AS orden
	FROM [Testing_ETL].[dbo].[Unificado] a
	)
DELETE FROM base_duplicada
WHERE orden > 1;

--Validacion
SELECT COUNT(1) AS cant,COUNT(DISTINCT CONCAT(id, muestra, resultado)) AS cant_unica FROM [Testing_ETL].[dbo].[Unificado];
--cant	cant_unica
--1805	1805

--######################################################################--
--CREACION DE TABLA TEMPORAL
--######################################################################--

SELECT *
INTO [Testing_ETL].[dbo].[Raw_Unificado]
FROM [Testing_ETL].[dbo].[Unificado]
WHERE 1 = 2;

ALTER TABLE [Testing_ETL].[dbo].[Raw_Unificado] DROP COLUMN FECHA_COPIA;

--######################################################################--
--CREACION DE TABLA DE LOG
--######################################################################--

CREATE TABLE [Testing_ETL].[dbo].[log_Unificado]
(
	fecha_proceso DATETIME,
	sp varchar(100),
	bd varchar(50),
	tabla_origen varchar(100),
	tabla_destino varchar(100),
	cant_filas int,
	cant_filas_unicas int,
	cant_filas_update int,
	cant_filas_insert int
);

SELECT * FROM [Testing_ETL].[dbo].[log_Unificado] ORDER BY fecha_proceso DESC;
