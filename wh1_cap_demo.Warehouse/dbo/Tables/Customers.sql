CREATE TABLE [dbo].[Customers] (
    [CustomerID] INT                                                              NOT NULL,
    [FirstName]  VARCHAR (50) MASKED WITH (FUNCTION = 'partial(1, "XXXXXXX", 0)') NULL,
    [LastName]   VARCHAR (50)                                                     NOT NULL,
    [Phone]      VARCHAR (20) MASKED WITH (FUNCTION = 'default()')                NULL,
    [Email]      VARCHAR (50) MASKED WITH (FUNCTION = 'email()')                  NULL
);


GO