CREATE TABLE [dbo].[sales_plan] (
    [ABP_FREIGHT]         INT            NULL,
    [ABP_REVENUE]         FLOAT (53)     NULL,
    [ABP_TONNAGE]         FLOAT (53)     NULL,
    [AREA_HEAD]           VARCHAR (8000) NULL,
    [CUS_GRP]             VARCHAR (8000) NULL,
    [CUS_SUBGRP]          VARCHAR (8000) NULL,
    [CUST_NAME]           VARCHAR (8000) NULL,
    [END_DATE]            FLOAT (53)     NULL,
    [ERDAT]               VARCHAR (8000) NULL,
    [ERNAM]               VARCHAR (8000) NULL,
    [ERZET]               VARCHAR (8000) NULL,
    [INCOTERM]            VARCHAR (8000) NULL,
    [KUNNR]               VARCHAR (8000) NULL,
    [PLANNED_FREIGHT]     INT            NULL,
    [PLANNED_REVENUE]     INT            NULL,
    [PLANNED_TONNAGE]     INT            NULL,
    [SALES_KAM]           VARCHAR (8000) NULL,
    [SALES_TYPE]          VARCHAR (8000) NULL,
    [SL_NO]               VARCHAR (8000) NULL,
    [SNOP_PRODUCT_GRP]    VARCHAR (8000) NULL,
    [START_DATE]          FLOAT (53)     NULL,
    [PlanDateForCalendar] VARCHAR (8000) NULL
);


GO