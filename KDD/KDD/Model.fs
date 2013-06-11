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
            
    type NameMatch = Full | Partial | Unmatched

    let matcher (n1:string []) (n2:string []) =

        let long1, short1 = n1 |> Array.map (fun c -> c, Unmatched) |> Array.partition (fun (c, _) -> c.Length > 1)
        let long2, short2 = n2 |> Array.map (fun c -> c, Unmatched) |> Array.partition (fun (c, _) -> c.Length > 1)
    
        long1 
        |> Array.iteri (fun i (c1, m1) ->
            if (long2 |> Array.exists (fun (c2, m2) -> c2 = c1 && (m2 = Unmatched))) 
            then
                let j = long2 |> Array.findIndex (fun (c2, m2) -> (c2 = c1) && (m2 = Unmatched))
                long1.[i] <- (c1, Full)
                long2.[j] <- (c1, Full)
            else ignore ())

        long1
        |> Array.iteri (fun i (c1, m1) ->
            if (short2 |> Array.exists (fun (c2, m2) -> c1.[0] = c2.[0] && (m2 = Unmatched)))
            then
                let j = short2 |> Array.findIndex (fun (c2, m2) -> c1.[0] = c2.[0] && (m2 = Unmatched))
                let c2 = fst short2.[j]
                long1.[i] <- (c1, Partial)
                short2.[j] <- (c2, Partial)
            else ignore ())

        long2
        |> Array.iteri (fun i (c1, m1) ->
            if (short1 |> Array.exists (fun (c2, m2) -> c1.[0] = c2.[0] && (m2 = Unmatched)))
            then
                let j = short1 |> Array.findIndex (fun (c2, m2) -> c1.[0] = c2.[0] && (m2 = Unmatched))
                let c2 = fst short1.[j]
                long2.[i] <- (c1, Partial)
                short1.[j] <- (c2, Partial)
            else ignore ())

        short1
        |> Array.iteri (fun i (c1, m1) ->
               if m1 = Unmatched
               then
                   if (short2 |> Array.exists (fun (c2, m2) -> c1.[0] = c2.[0] && (m2 = Unmatched)))
                   then
                       let j = short2 |> Array.findIndex (fun (c2, m2) -> c1.[0] = c2.[0] && (m2 = Unmatched))
                       short1.[i] <- (c1, Partial)
                       short2.[j] <- (c1, Partial)
                    else ignore ()
               else ignore ())
   
        if long1 |> Array.exists (fun (c, m) -> m = Unmatched) then false
        elif long2 |> Array.exists (fun (c, m) -> m = Unmatched) then false
        elif Array.append long1 long2 |> Array.exists (fun (c, m) -> m = Full) |> not then false
        elif short1 |> Array.exists (fun (c, m) -> m = Unmatched) && short2 |> Array.exists (fun (c, m) -> m = Unmatched) then false
        else true
