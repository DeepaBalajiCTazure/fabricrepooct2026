CREATE SECURITY POLICY [dbo].[SalesFilter]
    ADD FILTER PREDICATE [rls].[fn_securitypredicate]([SalesRep]) ON [dbo].[Sales]
    WITH (STATE = ON);


GO