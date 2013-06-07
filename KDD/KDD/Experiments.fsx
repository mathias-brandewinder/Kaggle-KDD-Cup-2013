#r @"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\Microsoft.VisualBasic.dll"
#load "Library1.fs"
#time

open System
open System.IO
open KDD
open KDD.Model

let authorsPath = @"Z:\Data\KDD-Cup\dataRev2\author.csv"
let authorsArticlesPath = @"Z:\Data\KDD-Cup\dataRev2\paperauthor.csv"

let authors = 
    let data = parseCsv authorsPath
    data.[1..]
    |> Array.map (fun x ->
        { AuthorId = Convert.ToInt32(x.[0]);
          Name = x.[1];
          Affiliation = x.[2] })

let papersAuthors =
    let data = parseCsv authorsArticlesPath
    data.[1..]
    |> Array.map (fun x ->
        { PaperId = Convert.ToInt32(x.[0]);
          AuthorId = Convert.ToInt32(x.[1]) })

let flattenPapers = 
    papersAuthors 
    |> Array.toSeq 
    |> Seq.groupBy (fun x -> x.PaperId)
    |> Seq.map (fun (id, authors) -> id, authors |> Seq.map (fun a -> a.AuthorId))
    |> Seq.map (fun (id, authors) -> sprintfn "%i, %s" id (String.Join(" ", authors))

let groups = 
    authors 
    |> Array.toSeq 
    |> Seq.groupBy (fun x -> x.Name.ToLowerInvariant())

let coAuthors (authorId:int) =
    let papers = 
        papersAuthors 
        |> Array.filter (fun x -> x.AuthorId = authorId)
        |> Array.map (fun x -> x.PaperId)
        |> Set.ofArray
    papersAuthors
    |> Array.filter (fun x -> papers.Contains x.PaperId)
    |> Array.map (fun x -> x.AuthorId)
    |> Set.ofArray 

authors.[0..20] 
|> Array.map (fun a -> 
    printfn "%i" (a.AuthorId) 
    coAuthors (a.AuthorId));;

papersAuthors 
|> Array.filter (fun x -> x.AuthorId = 14)

let combine (xs: int[]) =
    seq { for x in 0 .. (Array.length xs - 2) do
              for y in (x + 1) .. (Array.length xs - 1) do yield (xs.[x], xs.[y]) }

let name1 = [| "John"; "Doe" |]
let name2 = [| "J"; "A"; "Doe" |]

type Match = Full | Partial | Unmatched

let matcher (n1:string []) (n2:string []) =

    let matches1 = n1 |> Array.map (fun c -> c, Unmatched)
    let matches2 = n2 |> Array.map (fun c -> c, Unmatched)
    
    matches1 
    |> Array.iteri (fun i (c1, m1) ->
           if c1.Length > 1 
           then
               if (matches2 |> Array.exists (fun (c2, m2) -> c2 = c1 && (m2 = Unmatched))) 
               then
                   let j = matches2 |> Array.findIndex (fun (c2, m2) -> (c2 = c1) && (m2 = Unmatched))
                   matches1.[i] <- (c1, Full)
                   matches2.[j] <- (c1, Full)
                else ignore ()
            else ignore ())

    matches1
    |> Array.iteri (fun i (c1, m1) ->
           if c1.Length > 1 && m1 = Unmatched
           then
               if (matches2 |> Array.exists (fun (c2, m2) -> c2.Length = 1 && c1.[0] = c2.[0] && (m2 = Unmatched)))
               then
                   let j = matches2 |> Array.findIndex (fun (c2, m2) -> c2.Length = 1 && c1.[0] = c2.[0] && (m2 = Unmatched))
                   let c2 = fst matches2.[j]
                   matches1.[i] <- (c1, Partial)
                   matches2.[j] <- (c2, Partial)
                else ignore ()
           else ignore ())

    matches2
    |> Array.iteri (fun i (c1, m1) ->
           if c1.Length > 1 && m1 = Unmatched
           then
               if (matches1 |> Array.exists (fun (c2, m2) -> c2.Length = 1 && c1.[0] = c2.[0] && (m2 = Unmatched)))
               then
                   let j = matches1 |> Array.findIndex (fun (c2, m2) -> c2.Length = 1 && c1.[0] = c2.[0] && (m2 = Unmatched))
                   let c2 = fst matches1.[j]
                   matches2.[i] <- (c1, Partial)
                   matches1.[j] <- (c2, Partial)
                else ignore ()
           else ignore ())

    matches1
    |> Array.iteri (fun i (c1, m1) ->
           if c1.Length = 1 && m1 = Unmatched
           then
               if (matches2 |> Array.exists (fun (c2, m2) -> c2.Length = 1 && c1.[0] = c2.[0] && (m2 = Unmatched)))
               then
                   let j = matches2 |> Array.findIndex (fun (c2, m2) -> c2.Length = 1 && c1.[0] = c2.[0] && (m2 = Unmatched))
                   let c2 = fst matches2.[j]
                   matches1.[i] <- (c1, Partial)
                   matches2.[j] <- (c2, Partial)
                else ignore ()
           else ignore ())

    matches1, matches2

let matcher2 (n1:string []) (n2:string []) =

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
    [| for c in long1 do yield c
       for c in long2 do yield c
       for c in short1 do yield c
       for c in short2 do yield c |]
