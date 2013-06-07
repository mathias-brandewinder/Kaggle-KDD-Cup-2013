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
let coauthorsPath = @"Z:\Data\KDD-Cup\dataRev2\coauthors.csv"
let authorsArticlesPath = @"Z:\Data\KDD-Cup\dataRev2\paperauthor.csv"

let options = RegexOptions.Compiled ||| RegexOptions.IgnoreCase
let matchWords = new Regex(@"\w+", options)
let vocabulary (text: string) =
    matchWords.Matches(text)
    |> Seq.cast<Match>
    |> Seq.map (fun m -> m.Value)
    |> Seq.toArray
    //|> Set.ofSeq

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
let catalog = 
    let data = parseCsv authorsPath
    data.[1..]
    |> Array.map (fun x ->
        let id = Convert.ToInt32(x.[0])
        id,
        { AuthorId = id;
          Name = cleanup x.[1];
          Affiliation = x.[2] })
    |> Map.ofArray

//catalog |> Seq.map (fun kv -> kv.Value.Name) |> Seq.countBy id |> Seq.sortBy (fun (n, c) -> -c) |> Seq.take 50;;
//catalog |> Seq.filter (fun kv -> kv.Value.Name.Contains("  ")) |> Seq.toArray;;

let authors = 
    catalog
    |> Seq.map (fun kv ->
        let id = kv.Key
        let auth = kv.Value
        let name = auth.Name
        let chunks = vocabulary name
        let initials = chunks |> Array.map (fun c -> c.[0])
        let usableChunks = chunks |> Array.filter (fun c -> c.Length > 1)
        id, chunks, initials, usableChunks)

printfn "Preparing author Ids"
let authorIds = authors |> Seq.map (fun (id, _, _, _) -> id) |> Set.ofSeq

printfn "Preparing author initials"
let initials = authors  |> Seq.map (fun (id, _, initials, _) -> (id, initials)) |> Map.ofSeq

printfn "Preparing author 'name chunks'"
let words = authors  |> Seq.map (fun (id, chunks, _, _) -> (id, chunks)) |> Map.ofSeq

printfn "Preparing author 'long chunks'"
let names = authors  |> Seq.map (fun (id, _, _, chunks) -> (id, chunks)) |> Map.ofSeq

let lexicon (corpus:(string []) seq) =
    seq { for doc in corpus do
              for word in doc do yield word }
    |> Seq.countBy id
    |> Map.ofSeq

// improve: query on least frequent chunk
//let candidates (id:int) =
//    let myInitials = initials.[id]
//    let initialsMatches =
//        initials 
//        |> Map.toSeq
//        |> Seq.filter (fun (k, v) -> (Set.count v > 0) && ((Set.isSubset v myInitials) || (Set.isSubset myInitials v)))
//        |> Seq.map fst
//    let myChunks = names.[id]
//    initialsMatches
//    |> Seq.filter (fun x -> 
//        let chs = names.[x]
//        Set.exists (fun c -> myChunks.Contains(c)) chs)
//    |> Seq.toArray


type Match = Full | Partial | Unmatched

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

    let L = Array.append long1 long2

    if long1 |> Array.exists (fun (c, m) -> m = Unmatched) then false
    elif long2 |> Array.exists (fun (c, m) -> m = Unmatched) then false
    elif L |> Array.exists (fun (c, m) -> m = Full) |> not then false
    else true

let namesLexicon = words |> Map.toSeq |> Seq.map snd |> lexicon

let candidatesFor (id:int) =
    let target = words.[id]
    if Array.isEmpty target then [||]
    else
        let longName = target |> Array.minBy (fun w -> namesLexicon.[w])
        words 
        |> Seq.filter (fun kv -> kv.Value |> Array.exists (fun w -> w = longName))
        |> Seq.map (fun kv -> kv.Key, words.[kv.Key])
        |> Seq.filter (fun (id, words) -> matcher words target)
        |> Seq.toArray
      
let formatAuthor (author: (int*Set<int>)) =
    let line = String.Join(" ", (snd author))
    sprintf "%i, %s" (fst author) line

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

let cleverDupes (id:int) =
    printfn "%i" id
    let coauths = coAuthors id
    let candidates = 
        candidatesFor id 
        |> Array.map fst
        |> Array.filter (fun x -> 
            let cox = coAuthors x
            (Set.intersect coauths cox).Count > 0)
    id, candidates |> Set.ofArray |> Set.add id 

let test = 
    let ids = authorIds |> Seq.take 100 |> Seq.toArray
    ids |> Array.Parallel.map (fun id -> cleverDupes id |> formatAuthor)

let submitPath = @"C:\users\mathias\desktop\submit5.csv"
let submit = File.WriteAllLines(submitPath, (authorIds |> Set.toArray |> Array.Parallel.map (fun id -> cleverDupes id |> formatAuthor)))    