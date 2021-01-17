USE [my_exchange];
GO

IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE database_id = DB_ID('my_exchange') AND is_broker_enabled = 0x01)
BEGIN
	ALTER DATABASE [my_exchange] SET ENABLE_BROKER; -- ��������� ����������� ������ � ���� ������!
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.service_queues WHERE [name] = 'dajet-exchange-export-queue')
BEGIN
	CREATE QUEUE [dajet-exchange-export-queue] WITH POISON_MESSAGE_HANDLING (STATUS = OFF);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.services WHERE [name] = 'dajet-exchange-export-service')
BEGIN
	CREATE SERVICE [dajet-exchange-export-service] ON QUEUE [dajet-exchange-export-queue] ([DEFAULT]);
	--GRANT CONTROL ON SERVICE::[dajet-exchange-export-service] TO [dajet-exchange-user];
	--GRANT SEND ON SERVICE::[dajet-exchange-export-service] TO [PUBLIC];
END;
GO

DECLARE @handle AS UNIQUEIDENTIFIER = CAST('00000000-0000-0000-0000-000000000000' AS UNIQUEIDENTIFIER);

BEGIN DIALOG @handle
FROM SERVICE [dajet-exchange-export-service]
TO SERVICE 'dajet-exchange-export-service', 'CURRENT DATABASE'
ON CONTRACT [DEFAULT] WITH ENCRYPTION = OFF;

SELECT @handle;
GO

--END CONVERSATION 'F41429B9-0559-EB11-9C8F-408D5C93CC8E' WITH CLEANUP;
--DROP SERVICE [dajet-exchange-export-service];
--DROP QUEUE [dajet-exchange-export-queue];
