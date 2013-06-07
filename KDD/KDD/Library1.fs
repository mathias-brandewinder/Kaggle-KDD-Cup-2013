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

    let papersAuthors (filePath: string) =
        use reader = new TextFieldParser(filePath)
        reader.TextFieldType <- FieldType.Delimited
        reader.SetDelimiters(",")
        seq { while (not reader.EndOfData) do yield reader.ReadFields() }
        |> Seq.skip 1
        |> Seq.map (fun x -> Convert.ToInt32(x.[0]), Convert.ToInt32(x.[1]))
        |> Seq.toArray