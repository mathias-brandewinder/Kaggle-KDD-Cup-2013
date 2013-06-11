#r @"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\Microsoft.VisualBasic.dll"
#load "Model.fs"
#time

open System
open System.Text
open System.Globalization
open System.IO
open System.Text.RegularExpressions
open Microsoft.VisualBasic.FileIO
open KDD
open KDD.Model

// let root = @"Z:\Data\KDD-Cup\dataRev2\"
let root = @"C:\Users\Administrator\Desktop\dataRev2\"

let authorsPath = root + "author.csv"
let authorsArticlesPath = root + "paperauthor.csv"
let closeNamesPath = root + "veryclose.csv"

let options = RegexOptions.Compiled ||| RegexOptions.IgnoreCase
let matchWords = new Regex(@"\w+", options)
let vocabulary (text: string) =
    matchWords.Matches(text)
    |> Seq.cast<Match>
    |> Seq.map (fun m -> m.Value)
    |> Seq.toArray

let cleanupDiacritics (text:string) =
    let formD = text.Normalize(NormalizationForm.FormD)
    let test = 
        [| for c in formD do
            let cat = CharUnicodeInfo.GetUnicodeCategory(c)
            if (not (cat = UnicodeCategory.NonSpacingMark)) then yield c |]
    String(test).Normalize(NormalizationForm.FormC)

let removeExtraSpaces (text:string) =
    Regex.Replace(text, @"\s+", " ")

let mistakes = [
    "Grudzi   nski", "Grudzinski";
    "P   erez", "Perez";
    "GONZ   ALEZ", "GONZALEZ";
    "Kratochv   ol", "Kratochvol"
    "Jos   e", "Jose";
    "Mart   i", "Marti";
    "V   ictor", "Victor";
    "Rodr   iguez", "Rodriguez";
    "FERN   ANDEZ", "FERNANDEZ";
    "Horv   ath", "Horvath";
    "JURDZI   NSKI", "JURDZINSKI";
    "Garc   ia", "Garcia";
    "Garc   ia", "Garcia";
    "Joaqu   on", "Joaquon";
    "Mart   i", "Marti";
    "Garc   ia", "Garcia";
    "Quintana-Ort   i", "Quintana-Orti";
    "M   erouane", "Merouane";
    "M   erouane", "Merouane";
    "Jim   enez", "Jimenez";
    "FERN   ANDEZ", "FERNANDEZ";
    "Ram   irez", "Ramirez";
    "Mart   onez", "Martonez";
    "M   emin", "Memin";
    "Mart   inez", "Martinez";
    "Rodr   iguez", "Rodriguez" ] |> Map.ofList

let removeMistakes (text:string) =
    mistakes |> Map.fold (fun (t:string) k v -> t.Replace(k, v)) text

let cleanup (text:string) =
    (removeMistakes text)
        .ToLowerInvariant()
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

printfn "Preparing names lexicon"
let lexicon (corpus:(string []) seq) =
    seq { for doc in corpus do
              for word in doc do yield word }
    |> Seq.countBy id
    |> Map.ofSeq

let namesLexicon = words |> Map.toSeq |> Seq.map snd |> lexicon |> Map.add "kratochv" 1


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

printfn "Prepare misspellings table"

let misspellings = 
    seq { let pairs = parseCsv closeNamesPath |> Seq.map (fun line -> line.[0], line.[1])
          for (w1, w2) in pairs do 
              if w1.Length >= 5 && w2.Length >= 5 && w1.[0] = w2.[0] then
                  yield (w1, w2)
                  yield (w2, w1) }
    |> Seq.groupBy fst
    |> Seq.map (fun (w, ws) -> w, ws |> Seq.map snd |> Set.ofSeq )
    |> Map.ofSeq

printfn "Data fully loaded"

let matchesForName (target:string[]) =
    let target = target |> Array.filter (fun w -> w.Length > 1)
    if Array.isEmpty target then [||]    
    else
        let mostSpecificName = target |> Array.minBy (fun w -> namesLexicon.[w])
        if Array.length target > 1 && namesLexicon.[mostSpecificName] > 100 then
            let target = target |> Array.sortBy (fun w -> namesLexicon.[w])
            let w1 = target.[0]
            let w2 = target.[1]
            let i1 = (string)w1.[0]
            let i2 = (string)w2.[0]
            let s1 = [w1;w2] |> Set.ofList
            let s2 = [w1;i2] |> Set.ofList
            let s3 = [i1;w2] |> Set.ofList
            let isMatch s1 s2 s3 S =
                Set.isSuperset S s1 || Set.isSuperset S s2 || Set.isSuperset S s3
            words 
            |> Seq.filter (fun kv -> Set.ofArray kv.Value |> isMatch s1 s2 s3)
            |> Seq.map (fun kv -> kv.Key, words.[kv.Key])
            |> Seq.filter (fun (id, words) -> Model.matcher words target)
            |> Seq.toArray
        else
            words 
            |> Seq.filter (fun kv -> kv.Value |> Array.exists (fun w -> w = mostSpecificName))
            |> Seq.map (fun kv -> kv.Key, words.[kv.Key])
            |> Seq.filter (fun (id, words) -> matcher words target)
            |> Seq.toArray

let likelyMisspellingsFor (target:string[]) =
    if target = Array.empty then Seq.empty
    else
        let mostSpecificName = target |> Array.minBy (fun w -> namesLexicon.[w])
        if Map.containsKey mostSpecificName misspellings
        then 
            let index = Array.IndexOf(target, mostSpecificName) 
            misspellings.[mostSpecificName]
            |> Set.toSeq
            |> Seq.map (fun alt -> 
                   let copy = Array.copy target
                   copy.[index] <- alt
                   copy)
        else Seq.empty

let candidatesFor (id:int) =
    let target = words.[id]
    let misspelled = likelyMisspellingsFor target

    seq { yield! matchesForName target
          for alternate in misspelled do yield! (matchesForName alternate) }
    |> Seq.toArray
   
let coAuthors (authorId:int) =
    let found = Map.tryFind authorId papersByAuthor
    match found with
    | None -> Set.empty
    | Some(papers) -> 
        seq { for paperId in papers do 
                  let authors = authorsOfPaper.[paperId]
                  for authorId in authors do yield authorId }
        |> Set.ofSeq

let similarity (first:int Set) (second: int Set) =
    let inter = Set.intersect first second
    (float)(Set.count inter) / (float)(min (Set.count first) (Set.count second))

let cleverDupes (id:int) =
    printfn "%i" id
    let coauths = coAuthors id
    let candidates = 
        candidatesFor id 
        |> Array.map fst
        |> Array.filter (fun x -> 
            let cox = coAuthors x
            similarity coauths cox > 0.01)
    id, candidates |> Set.ofArray |> Set.add id 

// File creation

let formatAuthor (author: (int*Set<int>)) =
    let line = String.Join(" ", (snd author))
    sprintf "%i, %s" (fst author) line

let check (id:int) =
    let coauth = coAuthors id
    let dupes = cleverDupes id
    printfn "%i %s" id (catalog.[id].Name)
    dupes 
    |> snd
    |> Set.iter (fun d -> 
               let coauth = coAuthors id
               let other = coAuthors d
               printfn "    %i %s %.3f" d (catalog.[d].Name) (similarity coauth other))

let test = 
    let ids = authorIds |> Seq.take 1000 |> Seq.toArray
    ids 
    |> Array.Parallel.map (fun id -> cleverDupes id)
    |> Array.filter (fun (id, dupes) -> Set.count dupes > 1)
    |> Array.iter (fun (id, dupes) -> 
           printfn "%i %s" id (catalog.[id].Name)
           dupes 
           |> Set.iter (fun d -> 
               let coauth = coAuthors id
               let other = coAuthors d
               printfn "    %i %s %.3f" d (catalog.[d].Name) (similarity coauth other) ))



let results = authorIds |> Set.toArray |> Array.Parallel.map (fun id -> cleverDupes id |> formatAuthor)
let submitPath = root + "submit11.csv"
let submit = File.WriteAllLines(submitPath, results)  