#r @"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\Microsoft.VisualBasic.dll"
#load "Library1.fs"
#time

open System
open System.Text
open System.Globalization
open System.IO
open System.Text.RegularExpressions
open Microsoft.VisualBasic.FileIO
open KDD
open KDD.Model

let authorsPath = @"Z:\Data\KDD-Cup\dataRev2\author.csv"
let authorsArticlesPath = @"Z:\Data\KDD-Cup\dataRev2\paperauthor.csv"

let options = RegexOptions.Compiled ||| RegexOptions.IgnoreCase
let matchWords = new Regex(@"\w+", options)
let vocabulary (text: string) =
    matchWords.Matches(text)
    |> Seq.cast<Match>
    |> Seq.map (fun m -> m.Value)
    |> Set.ofSeq

let cleanupDiacritics (text:string) =
    let formD = text.Normalize(NormalizationForm.FormD)
    let test = 
        [| for c in formD do
            let cat = CharUnicodeInfo.GetUnicodeCategory(c)
            if (not (cat = UnicodeCategory.NonSpacingMark)) then yield c |]
    String(test).Normalize(NormalizationForm.FormC)

let removeExtraSpaces (text:string) =
    Regex.Replace(text, @"\s+", " ")

let cleanup (text:string) =
    text.ToLowerInvariant()
        .Replace("-", " ")
        .Replace(".", " ")
        .Replace(",", " ")
    |> removeExtraSpaces
    |> cleanupDiacritics

printfn "Reading authors"
let authors = 
    let data = parseCsv authorsPath
    data.[1..]
    |> Array.map (fun x ->
        let id = Convert.ToInt32(x.[0])
        id,
        { AuthorId = id;
          Name = cleanup x.[1];
          Affiliation = x.[2] })
    |> Map.ofArray

printfn "Reading papers / authors"
let papersAuthors =
    let data = parseCsv authorsArticlesPath
    data.[1..]
    |> Array.map (fun x ->
        { PaperId = Convert.ToInt32(x.[0]);
          AuthorId = Convert.ToInt32(x.[1]) })

printfn "Prepare papers by author"
let papersByAuthor =
    papersAuthors 
    |> Seq.groupBy (fun x -> x.AuthorId)
    |> Seq.map (fun (authorId, data) ->
        authorId, data |> Seq.map (fun x -> x.PaperId) |> Set.ofSeq)
    |> Map.ofSeq

printfn "Prepare authors by paper"
let authorsOfPaper =
    papersAuthors 
    |> Seq.groupBy (fun x -> x.PaperId)
    |> Seq.map (fun (paperId, data) ->
        paperId, data |> Seq.map (fun x -> x.AuthorId) |> Set.ofSeq)
    |> Map.ofSeq
    
let coAuthors (authorId:int) =
    let found = Map.tryFind authorId papersByAuthor
    match found with
    | None -> Set.empty
    | Some(papers) -> 
        seq { for paperId in papers do 
                  let authors = authorsOfPaper.[paperId]
                  for authorId in authors do yield authorId }
        |> Set.ofSeq
//        let coAuthors = papers |> Seq.map (fun paperId -> authorsOfPaper.[paperId])
//        Set.unionMany coAuthors

let merge (data: (int*Set<int>)[]) =
    seq {
        for x in data do
            let id, cos = x
            let merged =
                data 
                |> Array.filter (fun (_, coy) -> (Set.intersect cos coy).Count > 0)
                |> Array.map (fun (y, _) -> y)
                |> Set.ofArray
                |> Set.add (fst x)
            yield (id, merged) }
    |> Seq.toArray

let individuals (data: (int*Set<int>)[]) =
    let rec more (data: (int*Set<int>)[]) =
        let merged = merge data
        if merged = data then merged else more merged
    more data

let formatAuthor (author: (int*Set<int>)) =
    let line = String.Join(" ", (snd author))
    sprintf "%i, %s" (fst author) line

let groups = 
    authors 
    |> Map.toSeq 
    |> Seq.groupBy (fun (k, v) -> v.Name)

let smartDupes  = seq {
    for group in groups do
        let name = fst group
        if name = ""
        then
            let data = 
                (snd group)
                |> Seq.map (fun (id, auth) -> auth.AuthorId, [auth.AuthorId] |> Set.ofList)
                |> Seq.toArray
                |> individuals
            for author in data do
                yield (formatAuthor author)            
        else
            let data = 
                (snd group)
                |> Seq.map (fun (id, auth) -> auth.AuthorId, coAuthors auth.AuthorId)
                |> Seq.toArray
                |> individuals
            for author in data do
                yield (formatAuthor author) }

let submitPath = @"C:\users\mathias\desktop\submit4.csv"
let submit = File.WriteAllLines(submitPath, (smartDupes |> Seq.toArray))    