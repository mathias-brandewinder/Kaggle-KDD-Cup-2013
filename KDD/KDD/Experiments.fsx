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

