-- ================================================
-- Template generated from Template Explorer using:
-- Create Procedure (New Menu).SQL
--
-- Use the Specify Values for Template Parameters 
-- command (Ctrl-Shift-M) to fill in the parameter 
-- values below.
--
-- This block of comments will not be included in
-- the definition of the procedure.
-- ================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[run_raw_Unificado]
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @recordCount INT;
	DECLARE @recordCountDistinct INT;
	DECLARE @recordCountUpdate INT;

	SELECT
		@recordCount = COUNT(1),
		@recordCountDistinct = COUNT(DISTINCT CONCAT(id, muestra, resultado))
	FROM [Testing_ETL].[dbo].[Raw_Unificado];

	SELECT
		@recordCountUpdate = COUNT(1)
	FROM [Testing_ETL].[dbo].[Raw_Unificado] a
	INNER JOIN [Testing_ETL].[dbo].[Unificado] b ON a.ID = b.ID AND a.MUESTRA = b.MUESTRA AND a.RESULTADO = b.RESULTADO

	MERGE INTO [Testing_ETL].[dbo].[Unificado] AS a
	USING [Testing_ETL].[dbo].[Raw_Unificado] AS b
	ON a.ID = b.ID AND a.MUESTRA = b.MUESTRA AND a.RESULTADO = b.RESULTADO
	WHEN MATCHED THEN
		UPDATE SET
			a.CHROM = b.CHROM,
			a.POS = b.POS,
			a.REF = b.REF,
			a.ALT = b.ALT,
			a.QUAL = b.QUAL,
			a.[FILTER] = b.[FILTER],
			a.INFO = b.INFO,
			a.[FORMAT] = b.[FORMAT],
			a.VALOR = b.VALOR,
			a.ORIGEN = b.ORIGEN,
			a.FECHA_COPIA = CURRENT_TIMESTAMP
	WHEN NOT MATCHED THEN
		INSERT (CHROM, POS, ID, REF, ALT, QUAL, [FILTER], INFO, [FORMAT], MUESTRA, VALOR, ORIGEN, FECHA_COPIA, RESULTADO)
		VALUES (b.CHROM, b.POS, b.ID, b.REF, b.ALT, b.QUAL, b.[FILTER], b.INFO, b.[FORMAT], b.MUESTRA, b.VALOR, b.ORIGEN, CURRENT_TIMESTAMP, b.RESULTADO);

	INSERT INTO [Testing_ETL].[dbo].[log_Unificado]
	VALUES (CURRENT_TIMESTAMP, '[Testing_ETL].[dbo].[run_raw_Unificado]','Testing_ETL','[Testing_ETL].[dbo].[Raw_Unificado]','[Testing_ETL].[dbo].[Unificado]',@recordCount,@recordCountDistinct,@recordCountUpdate,@recordCountDistinct - @recordCountUpdate);
END;