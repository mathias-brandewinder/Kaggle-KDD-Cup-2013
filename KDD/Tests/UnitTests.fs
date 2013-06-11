module Test.``KDD Tests``

open NUnit.Framework
open FsUnit
open KDD
open KDD.Model

[<Test>]
let ``Matcher`` () =  

    let name1 = [| "john"; "doe" |]
    matcher name1 name1 |> should equal true

    let name2 = [| "j"; "doe" |]
    matcher name1 name2 |> should equal true

    let name3 = [| "jack"; "doe" |]
    matcher name1 name3 |> should equal false

    let name4 = [| "j"; "a"; "doe" |]
    matcher name1 name4 |> should equal true

    let name5 = [| "john"; "a"; "doe" |]
    matcher name1 name5 |> should equal true

    let name6 = [| "j"; "b"; "doe" |]
    matcher name4 name6 |> should equal false

