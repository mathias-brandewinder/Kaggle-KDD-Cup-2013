#r @"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\Microsoft.VisualBasic.dll"
#load "Library1.fs"
#time

open System
open System.IO
open System.Text.RegularExpressions
open Microsoft.VisualBasic.FileIO
open KDD
open KDD.Model

let authorsPath = @"Z:\Data\KDD-Cup\dataRev2\author.csv"
let coauthorsPath = @"Z:\Data\KDD-Cup\dataRev2\coauthors.csv"

let options = RegexOptions.Compiled ||| RegexOptions.IgnoreCase
let matchWords = new Regex(@"\w+", options)
let vocabulary (text: string) =
    matchWords.Matches(text)
    |> Seq.cast<Match>
    |> Seq.map (fun m -> m.Value)
    |> Set.ofSeq

printfn "Reading authors"
let authors = 
    let data = parseCsv authorsPath
    data.[1..]
    |> Array.map (fun x ->
        let id = Convert.ToInt32(x.[0])
        let name = x.[1].ToLowerInvariant()
        let chunks = vocabulary name
        let initials = chunks |> Set.map (fun c -> c.[0])
        let usableChunks = chunks |> Set.filter (fun c -> c.Length > 1)
        id, initials, usableChunks)

printfn "Preparing author Ids"
let authorIds = authors |> Seq.map (fun (id, _, _) -> id) |> Set.ofSeq

printfn "Preparing author initials"
let initials = authors  |> Seq.map (fun (id, initials, _) -> (id, initials)) |> Map.ofSeq

printfn "Preparing author 'name chunks'"
let names = authors  |> Seq.map (fun (id, _, chunks) -> (id, chunks)) |> Map.ofSeq
    
printfn "Retrieving co-authors"

let coAuthors =
    let dict = System.Collections.Generic.Dictionary<int,Set<int>>()
    let reader = new TextFieldParser(coauthorsPath)
    reader.TextFieldType <- FieldType.Delimited
    reader.SetDelimiters(",")
    let input = seq { while (not reader.EndOfData) do yield reader.ReadFields() }
    input
    |> Seq.iter (fun line ->        
        let id = Convert.ToInt32(line.[0])
        printfn "%i" id
        let coauths = (line.[1]).Split(' ') |> Array.map (fun x -> Convert.ToInt32(x)) |> Set.ofArray
        dict.Add(id, coauths))
    dict
    
let candidatesFor (id:int) =
    if (not (coAuthors.ContainsKey(id)))
    then Set.empty
    else
        let coauths = coAuthors.[id]
        seq { 
            for coauthId in coauths do
                let nextLevel = coAuthors.[coauthId]
                for candidate in nextLevel do
                    if (not (coauths.Contains(candidate))) then yield candidate }
        |> Set.ofSeq

// If something matches, true
let minMatch (s1:_ Set) (s2: _ Set) =
    s1 |> Set.exists (fun s -> s2.Contains(s))

// If more than n matches, true
let nMatch (n:int) (s1:_ Set) (s2: _ Set) =
    Set.intersect s1 s2 |> Set.count |> (<=) n

// String-edit distance
let levenshtein (src: string) (target: string) =
    let min3 a b c = min a (min b c)
    let m,n = src.Length, target.Length
    let prev = Array.init (n+1) id
    let nxt = Array.zeroCreate (n+1)
    for i in 1..m do
        nxt.[0] <- i
        for j in 1..n do
            if src.[i - 1] = target.[j - 1] then
                nxt.[j] <- prev.[j - 1]
            else
                nxt.[j] <- min3 (prev.[j] + 1)
                                (nxt.[j - 1] + 1)
                                (prev.[j - 1] + 1)
        Array.blit nxt 0 prev 0 (n+1)
    nxt.[n]

let namesOverlap (n1:string Set) (n2: string Set) =
    n1 
    |> Seq.map (fun chunk -> 
        n2 
        |> Seq.map (fun c -> levenshtein c chunk)
        |> Seq.min)
    |> Seq.filter (fun x -> x < 2)
    |> Seq.length

let duplicates (id:int) (candidates:int Set) =
    let myInitials = initials.[id]
    let myChunks = names.[id]
    seq { for candidate in candidates do
              if authorIds.Contains(candidate) then
                  if (nMatch 2 myInitials (initials.[candidate])) then 
                      if ((namesOverlap names.[id] myChunks) > 1) then yield candidate }

let test () =
    let x = authorIds |> Seq.take 100        
    for i in x do 
        let c = candidatesFor i
        printfn "Id: %i, dupes: %i" i (duplicates i c |> Set.ofSeq |> Set.count)

//// find co-authors of co-authors
//let merge (data: (int*Set<int>)[]) =
//    seq {
//        for x in data do
//            let id, cos = x
//            let merged =
//                data 
//                |> Array.filter (fun (_, coy) -> (Set.intersect cos coy).Count > 0)
//                |> Array.map (fun (y, _) -> y)
//                |> Set.ofArray
//                |> Set.add (fst x)
//            yield (id, merged) }
//    |> Seq.toArray
//
//let individuals (data: (int*Set<int>)[]) =
//    let rec more (data: (int*Set<int>)[]) =
//        let merged = merge data
//        if merged = data then merged else more merged
//    more data

let formatAuthor (author: (int*Set<int>)) =
    let line = String.Join(" ", (snd author))
    sprintf "%i, %s" (fst author) line

//let smartDupes  = seq {
//    for group in groups do
//        let data = 
//            (snd group)
//            |> Seq.map (fun auth -> auth.AuthorId, coAuthors auth.AuthorId)
//            |> Seq.toArray
//            |> individuals
//        for author in data do
//            yield (formatAuthor author) }
//
//let authorIds = authors |> Array.map (fun x -> x.AuthorId) |> Set.ofArray
//let candidates (authorId:int) =
//    let excluded = coAuthors authorId
//    seq { for excl in excluded do 
//              let coExcl = coAuthors excl
//              for candidate in coExcl do yield candidate }
//    |> Set.ofSeq
//    |> Set.filter (fun x -> authorIds.Contains(x))
//    |> Set.filter (fun x -> not (excluded.Contains(x)))

//authors.[0..99] |> Array.map (fun a -> candidates (a.AuthorId)) |> Array.iter (fun s -> printfn "%i candidates" (Set.count s))
//
//let test (authorIndex:int) =
//    let author = authors |> Array.find (fun x -> x.AuthorId = authorIndex)//authors.[authorIndex]
//    printfn "%s" author.Name
//    printfn ""
//    candidates (author.AuthorId) 
//    |> Seq.map (fun id -> 
//        let co = authors |> Array.find (fun x -> x.AuthorId = id) 
//        co.Name)
//    |> Seq.sort
//    |> Seq.iter (fun name -> printfn "%s" name)
//
//
//
//let test2 (authorId:int) =
//    let author = authors |> Array.find (fun x -> x.AuthorId = authorId)//authors.[authorIndex]
//    let authorInitials = initials.[authorId]
//    printfn "%s" author.Name
//    printfn ""
//    candidates (author.AuthorId) 
//    |> Set.filter (fun x -> (Set.intersect initials.[x] authorInitials).Count > 0)
//    |> Seq.map (fun id -> 
//        let co = authors |> Array.find (fun x -> x.AuthorId = id) 
//        co.Name)
//    |> Seq.sort
//    |> Seq.iter (fun name -> printfn "%s" name)
//
//let submitPath = @"C:\users\mathias\desktop\submit3.csv"
//let submit = File.WriteAllLines(submitPath, (smartDupes |> Seq.toArray))    