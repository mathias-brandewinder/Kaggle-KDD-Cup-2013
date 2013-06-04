#r @"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\Microsoft.VisualBasic.dll"
#load "Library1.fs"

open System
open System.IO
open System.Text.RegularExpressions
open KDD
open KDD.Model

let authorsArticlesPath = @"Z:\Data\KDD-Cup\dataRev2\paperauthor.csv"

let options = RegexOptions.Compiled ||| RegexOptions.IgnoreCase
let matchWords = new Regex(@"\w+", options)
let vocabulary (text: string) =
    matchWords.Matches(text)
    |> Seq.cast<Match>
    |> Seq.map (fun m -> m.Value)
    |> Set.ofSeq

printfn "Reading authors / papers"
let papersAuthors =
    let data = parseCsv authorsArticlesPath
    data.[1..]
    |> Array.map (fun x ->
        { PaperId = Convert.ToInt32(x.[0]);
          AuthorId = Convert.ToInt32(x.[1]) })

printfn "Retrieving papers by author"
let papersByAuthor =
    papersAuthors 
    |> Seq.groupBy (fun x -> x.AuthorId)
    |> Seq.map (fun (authorId, data) ->
        authorId, data |> Seq.map (fun x -> x.PaperId) |> Set.ofSeq)
    |> Map.ofSeq

printfn "Retrieving authors by paper"
let authorsOfPaper =
    papersAuthors 
    |> Seq.groupBy (fun x -> x.PaperId)
    |> Seq.map (fun (paperId, data) ->
        paperId, data |> Seq.map (fun x -> x.AuthorId) |> Set.ofSeq)
    |> Map.ofSeq
    
let resultFile = @"Z:\Data\KDD-Cup\dataRev2\coauthors.csv"

let writer = new StreamWriter(resultFile)
let append (line: string) = writer.WriteLine(line)

let flatten (x: (int * (Set<int>))) = 
    let (id, coauths) = x
    let flat = coauths |> Set.toArray |> Array.map (fun x -> x.ToString())
    sprintf "%i,%s" id (String.Join(" ", flat))

papersByAuthor
|> Seq.iter (fun kv ->
        let authorId, paperIds = kv.Key, kv.Value
        printfn "%i" authorId
        let coauthors = 
            seq { for paperId in paperIds do
                      let coAuthors = authorsOfPaper.[paperId]
                      for coAuthId in coAuthors do yield coAuthId }
            |> Set.ofSeq
        flatten (authorId, coauthors) |> append)

printfn "Done"
(writer.Close()) |> ignore