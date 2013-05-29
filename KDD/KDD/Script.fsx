#r @"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\Microsoft.VisualBasic.dll"
#load "Library1.fs"
#time

open System
open System.IO
open KDD
open KDD.Model

let authorsPath = @"Z:\Data\KDD-Cup\dataRev2\author.csv"

let authors = 
        let data = parseCsv authorsPath
        data.[1..]
        |> Array.map (fun x ->
            { AuthorId = Convert.ToInt32(x.[0]);
              Name = x.[1];
              Affiliation = x.[2] })

let groups = 
    authors 
    |> Array.toSeq 
    |> Seq.groupBy (fun x -> x.Name.ToLowerInvariant())

let dupes = seq {
    for group in groups do
        let (name, data) = group
        let akas = data |> Seq.map (fun author -> author.AuthorId) |> Seq.toArray
        let line = String.Join(" ", akas)
        for aka in akas do
            yield sprintf "%i, %s" aka line }

let submitPath = @"C:\users\mathias\desktop\submit.csv"
let submit = File.WriteAllLines(submitPath, (dupes |> Seq.toArray))    