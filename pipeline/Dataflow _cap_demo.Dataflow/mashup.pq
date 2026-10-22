[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "orders_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "CustomerID", DestinationColumnName = "CustomerID"], [SourceColumnName = "Count", DestinationColumnName = "Count"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared orders = let
  Source = Csv.Document(Web.Contents("https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/orders.csv"), [Delimiter = ",", Columns = 7, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"SalesOrderID", Int64.Type}, {"OrderDate", type date}, {"CustomerID", Int64.Type}, {"LineItem", Int64.Type}, {"ProductID", Int64.Type}, {"OrderQty", Int64.Type}, {"LineItemTotal", type number}}),
  #"Choose columns" = Table.SelectColumns(#"Changed column type", {"OrderDate", "CustomerID", "LineItem", "ProductID", "OrderQty", "LineItemTotal"}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Choose columns", "month", each Date.Month([OrderDate])), {{"month", Int64.Type}}),
  #"Renamed columns" = Table.RenameColumns(#"Added custom", {{"OrderQty", "OrderQuantity"}}),
  #"Choose columns 1" = Table.SelectColumns(#"Renamed columns", {"CustomerID", "OrderQuantity"}),
  #"Grouped rows" = Table.Group(#"Choose columns 1", {"CustomerID"}, {{"Count", each Table.RowCount(_), Int64.Type}}),
  #"Sorted rows" = Table.Sort(#"Grouped rows", {{"Count", Order.Descending}})
in
  #"Sorted rows";
shared orders_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "7907f786-2ea6-4931-8673-3671cc769255"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8c38520b-e35a-4e64-967e-b7b5c78a7acc"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "ordersaggregated", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
