#r @"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\Microsoft.VisualBasic.dll"
#load "Library1.fs"
#time

open System
open System.IO
open System.Text.RegularExpressions
open KDD
open KDD.Model

let authorsPath = @"Z:\Data\KDD-Cup\dataRev2\author.csv"
let authorsArticlesPath = @"Z:\Data\KDD-Cup\dataRev2\paperauthor.csv"

let cleanName (name:string) =
    name.Replace(".", "").Replace("-", " ").ToLowerInvariant()

let authors = 
    let data = parseCsv authorsPath
    data.[1..]
    |> Array.map (fun x ->
        { AuthorId = Convert.ToInt32(x.[0]);
          Name = cleanName x.[1];
          Affiliation = x.[2] })

let papersAuthors =
    let data = parseCsv authorsArticlesPath
    data.[1..]
    |> Array.map (fun x ->
        { PaperId = Convert.ToInt32(x.[0]);
          AuthorId = Convert.ToInt32(x.[1]) })

let groups = 
    authors 
    |> Array.toSeq 
    |> Seq.groupBy (fun x -> x.Name)

let papersByAuthor =
    papersAuthors 
    |> Seq.groupBy (fun x -> x.AuthorId)
    |> Seq.map (fun (authorId, data) ->
        authorId, data |> Seq.map (fun x -> x.PaperId) |> Set.ofSeq)
    |> Map.ofSeq

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
        let coAuthors = papers |> Seq.map (fun paperId -> authorsOfPaper.[paperId])
        Set.unionMany coAuthors

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

let smartDupes  = seq {
    for group in groups do
        let data = 
            (snd group)
            |> Seq.map (fun auth -> auth.AuthorId, coAuthors auth.AuthorId)
            |> Seq.toArray
            |> individuals
        for author in data do
            yield (formatAuthor author) }

let authorIds = authors |> Array.map (fun x -> x.AuthorId) |> Set.ofArray
let candidates (authorId:int) =
    let excluded = coAuthors authorId
    seq { for excl in excluded do 
              let coExcl = coAuthors excl
              for candidate in coExcl do yield candidate }
    |> Set.ofSeq
    |> Set.filter (fun x -> authorIds.Contains(x))
    |> Set.filter (fun x -> not (excluded.Contains(x)))

authors.[0..99] |> Array.map (fun a -> candidates (a.AuthorId)) |> Array.iter (fun s -> printfn "%i candidates" (Set.count s))

let test (authorIndex:int) =
    let author = authors |> Array.find (fun x -> x.AuthorId = authorIndex)//authors.[authorIndex]
    printfn "%s" author.Name
    printfn ""
    candidates (author.AuthorId) 
    |> Seq.map (fun id -> 
        let co = authors |> Array.find (fun x -> x.AuthorId = id) 
        co.Name)
    |> Seq.sort
    |> Seq.iter (fun name -> printfn "%s" name)


let options = RegexOptions.Compiled ||| RegexOptions.IgnoreCase
let matchWords = new Regex(@"\w+", options)
let vocabulary (text: string) =
    matchWords.Matches(text)
    |> Seq.cast<Match>
    |> Seq.map (fun m -> m.Value)
    |> Set.ofSeq

let initials (chunks:Set<string>) = chunks |> Set.map (fun x -> x.Substring(0,1))

let submitPath = @"C:\users\mathias\desktop\submit3.csv"
let submit = File.WriteAllLines(submitPath, (smartDupes |> Seq.toArray))    