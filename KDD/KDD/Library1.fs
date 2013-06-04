namespace KDD

type Author = { AuthorId:int; Name:string; Affiliation:string }
type AuthorPaper = { AuthorId:int; PaperId:int }

module Model =

    open System
    open Microsoft.VisualBasic.FileIO

    let parseCsv (filePath: string) =
        use reader = new TextFieldParser(filePath)
        reader.TextFieldType <- FieldType.Delimited
        reader.SetDelimiters(",")
        [| while (not reader.EndOfData) do yield reader.ReadFields() |]   