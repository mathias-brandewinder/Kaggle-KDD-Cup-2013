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

printfn "Preparing author 'long chunks'"
let names = authors  |> Seq.map (fun (id, _, _, chunks) -> (id, chunks)) |> Map.ofSeq

printfn "Preparing names lexicon"
let lexicon (corpus:(string []) seq) =
    seq { for doc in corpus do
              for word in doc do yield word }
    |> Seq.countBy id
    |> Map.ofSeq

let namesLexicon = words |> Map.toSeq |> Seq.map snd |> lexicon

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
   
    if long1 |> Array.exists (fun (c, m) -> m = Unmatched) then false
    elif long2 |> Array.exists (fun (c, m) -> m = Unmatched) then false
    elif Array.append long1 long2 |> Array.exists (fun (c, m) -> m = Full) |> not then false
    elif short1 |> Array.exists (fun (c, m) -> m = Unmatched) && short2 |> Array.exists (fun (c, m) -> m = Unmatched) then false
    else true

let matchesForName (target:string[]) =
    if Array.isEmpty target then [||]
    else
        let mostSpecificName = target |> Array.minBy (fun w -> namesLexicon.[w])
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

// File creation

let formatAuthor (author: (int*Set<int>)) =
    let line = String.Join(" ", (snd author))
    sprintf "%i, %s" (fst author) line

//let test = 
//    let ids = authorIds |> Seq.take 1000 |> Seq.toArray
//    ids 
//    |> Array.Parallel.map (fun id -> cleverDupes id)
//    |> Array.filter (fun (id, dupes) -> Set.count dupes > 1)
//    |> Array.iter (fun (id, dupes) -> 
//           printfn "%i %s" id (catalog.[id].Name)
//           dupes |> Set.iter (fun d -> printfn "    %i %s" d (catalog.[d].Name)))



let results = authorIds |> Set.toArray |> Array.Parallel.map (fun id -> cleverDupes id |> formatAuthor)
let submitPath = root + "submit8.csv"
let submit = File.WriteAllLines(submitPath, results)  