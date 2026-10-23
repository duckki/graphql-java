-- Adapted from graphql-incremental-delivery-js/fuzz/lean/NativeOracle.lean.
-- The adapter imports execution and queue algorithms unchanged from graphql-lean.
import Lean
import GraphQL.IncrementalDelivery.WorkQueueAlternativeImpl
import Proofs.GraphQL.IncrementalDelivery.WorkQueueAlternativeImpl.ScheduledExecutor

/-! JSON-lines adapter. Execution, queue transitions, and wire mapping are imported
unchanged from the supplied Lean model; only decoding, serialization, and replay live here.
TaskOutcome.deliveryResult selects delivery causes while retaining collected diagnostics.
The eager public policy stays the default. The separately proved finite sequential
cut policy is selected explicitly; neither executor is implemented in this adapter. -/
namespace NativeOracle
open Lean (Json toJson)
open GraphQL (Schema Name InputValue ConstInputValue TypeRef FieldDefinition TypeDefinition)
open GraphQL.IncrementalDelivery
open GraphQL.IncrementalDelivery.Execution

private def obj := Json.mkObj
private def arr (xs : List Json) := Json.arr xs.toArray
private def str := Json.str
private def num (n : Nat) := toJson n
private def get (j : Json) (key : String) := j.getObjVal? key
private def text (j : Json) (key : String) := j.getObjValAs? String key
private def entries (j : Json) : Except String (List (String × Json)) := do
  match j with
  | .obj fields => pure fields.toList
  | _ => throw "expected an object"

partial def input (j : Json) : Except String InputValue := do
  match j with
  | .null => pure .null
  | .bool b => pure (.boolean b)
  | .str s => pure (.string s)
  | .num _ => pure (.int (← j.getInt?))
  | .arr xs => pure (.list (← xs.toList.mapM input))
  | .obj _ => pure (.variable (← text j "variable"))

def constant (j : Json) : Except String ConstInputValue := do
  match ← input j with
  | .null => pure .null
  | .boolean b => pure (.boolean b)
  | .string s => pure (.string s)
  | .int n => pure (.int n)
  | _ => throw "unsupported runtime variable"

partial def typeRef (j : Json) : Except String TypeRef := do
  match j with
  | .str s => pure (.named s)
  | _ =>
      let xs ← j.getArr?
      if xs.size != 2 then throw "invalid type wrapper"
      match ← xs[0]!.getStr? with
      | "list" => pure (.list (← typeRef xs[1]!))
      | "nonNull" => pure (.nonNull (← typeRef xs[1]!))
      | _ => throw "unknown type wrapper"

def names (j : Json) (key : String) : Except String (List Name) := do
  match (get j key).toOption with
  | none => pure []
  | some value => (← value.getArr?).toList.mapM Json.getStr?

def typeDefinition (item : Json) : Except String TypeDefinition := do
  let name ← text item "name"
  let fields ← (← (← get item "fields").getArr?).toList.mapM fun field => do
    pure ({ name := ← text field "name", outputType := ← typeRef (← get field "type") } : FieldDefinition)
  let interfaces ← names item "interfaces"
  let kind ← match (get item "kind").toOption with
    | none => pure "object"
    | some value => value.getStr?
  match kind with
  | "object" => pure (.object { name, fields, interfaces })
  | "interface" => pure (.interface { name, fields, interfaces })
  | "union" => pure (.union { name, members := ← names item "members" })
  | _ => throw "unsupported type definition"

def schema (j : Json) : Except String Schema := do
  let types ← (← j.getArr?).toList.mapM typeDefinition
  pure { queryType := "Query", types }

def directive (j : Json) : Except String DirectiveApplication := do
  let name ← text j "name"
  let condition ← input (← get j "if")
  let label := match (get j "label").toOption with
    | some .null => some InputValue.null
    | some (.str value) => some (InputValue.string value)
    | _ => none
  match name with
  | "skip" => pure (.skip condition)
  | "include" => pure (.include condition)
  | "defer" => pure (.defer condition label)
  | "stream" => pure (.stream condition label (← input (← get j "initialCount")))
  | _ => throw "unknown directive"

partial def selection (j : Json) : Except String Selection := do
  let dirs ← (← (← get j "directives").getArr?).toList.mapM directive
  let children ← (← (← get j "selections").getArr?).toList.mapM selection
  match ← text j "kind" with
  | "field" => pure (.field (← text j "responseName") (← text j "name") [] dirs children)
  | "fragment" => pure (.inlineFragment (j.getObjValAs? String "typeCondition").toOption dirs children)
  | _ => throw "unknown selection"

instance : Inhabited (GraphQL.Execution.ResolverValue Json) := ⟨.null⟩

partial def value (j : Json) : GraphQL.Execution.ResolverValue Json :=
  match j with
  | .null => .null
  | .str s => .scalar s
  | .arr xs => .list (xs.toList.map value)
  | _ => .object ((text j "__typename").toOption.getD "Item") j

def resolve (_parent field : Name) (_args : GraphQL.Execution.CoercedArguments)
    (source : GraphQL.Execution.ResolverValue Json) : Option (GraphQL.Execution.ResolverValue Json) :=
  match source with
  | .object _ ref =>
      let result := (get ref field).toOption.getD .null
      if (result.getObjValAs? Bool "$error").toOption == some true then none else some (value result)
  | _ => none

def resolvers : GraphQL.Execution.Resolvers Json :=
  { resolve, resolve_argumentsEquivalent := by intros; rfl }

partial def responseValue : ResponseValue → Json
  | .null => .null
  | .scalar s => str s
  | .list xs => arr (xs.map responseValue)
  | .object fields => obj (fields.map fun (k, v) => (k, responseValue v))

def path (p : ResponsePath) : Json := arr (p.map fun
  | .field s => str s
  | .index n => num n)

def pending (p : IncrementalPendingNotice) : Json :=
  obj ([ ("id", str p.id), ("path", path p.path) ] ++ match p.label with
    | some (.string s) => [("label", str s)]
    | some .null => [("label", .null)]
    | none => [])

def patch : IncrementalResult → Json
  | .object id fields errors subPath => obj [
      ("id", str id), ("data", responseValue (.object fields)), ("errors", num errors), ("subPath", path subPath)]
  | .list id items errors => obj [("id", str id), ("items", arr (items.map responseValue)), ("errors", num errors)]

def update (u : IncrementalStreamUpdateResult) : Json := obj [
  ("hasNext", toJson u.hasNext), ("pending", arr (u.pending.map pending)),
  ("incremental", arr (u.incremental.map patch)),
  ("completed", arr (u.completed.map fun c => obj [("id", str c.id), ("errors", num c.errors)]))]

open GraphQL.IncrementalDelivery.WorkQueueSemantics (Occurrence)
abbrev R := ReferenceWorkQueue.State
abbrev A := AlternativeWorkQueue.State

def occurrence : Occurrence → Json
  | .executionGroup address => obj [("kind", str "task"), ("address", toJson address)]
  | .item address index => obj [("kind", str "item"), ("address", toJson address), ("index", num index)]

def descriptor (n : DeliveryNode) : Json := obj [
  ("key", num n.ref), ("path", path n.path),
  ("label", match n.label with | some (.string s) => str s | _ => .null)]

def groupValue (v : ExecutionGroupValue) : Json := obj [
  ("path", path v.path), ("data", responseValue (.object v.data)), ("errors", num v.errors),
  ("deliveryGroups", arr (v.deliveryGroups.map (num ∘ DeliveryNode.ref)))]
def itemValue (v : StreamItemValue) : Json := obj [("item", responseValue v.item), ("errors", num v.errors)]

def result (f : α → Json) : Result α → Json
  | .error errors => obj [("ok", toJson false), ("errors", num errors)]
  | .ok (v, errors) => obj [("ok", toJson true), ("value", f v), ("errors", num errors)]

-- Serialize the model's executed work. No execution or queue algorithm is copied here.
partial def work (w : Work) (address : List Nat := []) : Json :=
  match w with
  | .empty => obj [("kind", str "empty")]
  | .combine l r => obj [("kind", str "combine"), ("left", work l (address ++ [0])), ("right", work r (address ++ [1]))]
  | .declareGroups groups tasks => obj [
      ("kind", str "declare"),
      ("groups", arr (groups.map fun g => obj [("node", descriptor g.node), ("ancestors", arr (g.ancestors.map descriptor))])),
      ("children", work tasks address)]
  | .executionGroup groups p outcome children => obj [
      ("kind", str "task"), ("address", toJson address), ("path", path p),
      ("groups", arr (groups.map fun g => obj [("node", descriptor g.node), ("ancestors", arr (g.ancestors.map descriptor))])),
      ("result", result (fun fields => responseValue (.object fields)) outcome.deliveryResult),
      ("children", work children (address ++ [0]))]
  | .stream node items => obj [
      ("kind", str "stream"), ("address", toJson address), ("node", descriptor node),
      ("items", arr ((items.zipIdx).map fun ((r, children), i) => obj [
        ("result", result responseValue r.deliveryResult), ("children", work children (address ++ [i]))]))]

partial def taskAt (w : Work) (address target : List Nat) : Option (List DeferredFragment × ResponsePath × Result (List (Name × ResponseValue))) :=
  match w with
  | .empty => none
  | .combine l r => (taskAt l (address ++ [0]) target).orElse fun _ => taskAt r (address ++ [1]) target
  | .declareGroups _ tasks => taskAt tasks address target
  | .executionGroup gs p r children => if address == target then some (gs, p, r.deliveryResult) else taskAt children (address ++ [0]) target
  | .stream _ items => (items.zipIdx).findSome? fun (item, i) => taskAt item.2 (address ++ [i]) target

partial def streamAt (w : Work) (address target : List Nat) : Option (DeliveryNode × List (Result ResponseValue × Work)) :=
  match w with
  | .empty => none
  | .combine l r => (streamAt l (address ++ [0]) target).orElse fun _ => streamAt r (address ++ [1]) target
  | .declareGroups _ tasks => streamAt tasks address target
  | .executionGroup _ _ _ children => streamAt children (address ++ [0]) target
  | .stream n items => if address == target then some (n, items.map fun (r, w) => (r.deliveryResult, w)) else
      (items.zipIdx).findSome? fun (item, i) => streamAt item.2 (address ++ [i]) target

partial def groupAt (w : Work) (ref : NodeRef) : Option DeliveryNode :=
  match w with
  | .empty => none
  | .combine l r => (groupAt l ref).orElse fun _ => groupAt r ref
  | .declareGroups gs tasks =>
      ((gs.find? fun g => g.node.ref == ref).map DeferredFragment.node).orElse fun _ => groupAt tasks ref
  | .executionGroup gs _ _ children =>
      ((gs.find? fun g => g.node.ref == ref).map DeferredFragment.node).orElse fun _ => groupAt children ref
  | .stream _ items => items.findSome? fun item => groupAt item.2 ref

def event (w : Work) (j : Json) : Except String ReferenceWorkQueue.GraphEvent := do
  if (← text j "kind") == "group-failure" then
    let ref ← j.getObjValAs? Nat "group"
    let errors ← j.getObjValAs? Nat "errors"
    let some node := groupAt w ref | throw "unknown group"
    return .groupFailure { group := node, errors }
  let address ← j.getObjValAs? (List Nat) "address"
  if (← text j "kind") == "task" then
    let some (gs, p, r) := taskAt w [] address | throw "unknown task"
    match r with
    | .error errors => pure (.taskFailure (.executionGroup address) errors)
    | .ok (data, errors) =>
        let some children := ReferenceWorkQueue.taskChildWork? w (.executionGroup address) | throw "missing task work"
        pure (.taskSuccess (.executionGroup address) { value := { path := p, data, errors, deliveryGroups := gs.map DeferredFragment.node }, work := children })
  else
    let some (node, items) := streamAt w [] address | throw "unknown stream"
    match ← text j "kind" with
    | "stream-success" => pure (.streamSuccess node)
    | "stream-failure" =>
        let index ← j.getObjValAs? Nat "index"
        let some (.error errors, _) := items[index]? | throw "not a failed stream item"
        pure (.streamFailure node errors)
    | "stream-items" =>
        let index ← j.getObjValAs? Nat "index"
        let count ← j.getObjValAs? Nat "count"
        if count == 0 then throw "empty stream batch"
        let incoming ← (List.range count).mapM fun offset => do
          let i := index + offset
          let some (.ok (v, errors), _) := items[i]? | throw "not a successful stream item"
          let some children := ReferenceWorkQueue.streamItemWork? w (.item address i) | throw "missing item work"
          pure ({ occurrence := .item address i, value := { item := v, errors }, work := children } : ReferenceWorkQueue.StreamItem)
        pure (.streamItems node incoming)
    | _ => throw "unknown graph event"

-- Deterministic host scheduling only; all reductions remain imported model calls.
partial def streamByRef (w : Work) (ref : Nat) (address : List Nat := []) :
    Option (List Nat × DeliveryNode × List (Result ResponseValue × Work)) :=
  match w with
  | .empty => none
  | .combine l r => (streamByRef l ref (address ++ [0])).orElse fun _ => streamByRef r ref (address ++ [1])
  | .declareGroups _ tasks => streamByRef tasks ref address
  | .executionGroup _ _ _ children => streamByRef children ref (address ++ [0])
  | .stream node items => if node.ref == ref then some (address, node, items.map fun (outcome, children) => (outcome.deliveryResult, children)) else
      (items.zipIdx).findSome? fun (item, i) => streamByRef item.2 ref (address ++ [i])

def nextAutomaticInput (w : Work) (state : A) (cursors : List (Nat × Nat)) :
    Except String (Json × List (Nat × Nat)) := do
  if let some task := state.queue.taskNodes.find? (fun task => task.value.isNone && task.rejection.isNone) then
    match task.task.occurrence with
    | .executionGroup address => return (obj [("kind", str "task"), ("address", toJson address)], cursors)
    | _ => throw "unexpected non-task occurrence"
  if let some failure := state.queue.groupFailureEvents.head? then
    return (obj [("kind", str "group-failure"), ("group", num failure.group.ref),
      ("errors", num failure.errors)], cursors)
  let some ref := state.queue.rootStreams.head? | throw "no eligible model input"
  let some (address, _, items) := streamByRef w ref | throw "unknown model stream"
  let index := ((cursors.find? (fun pair => pair.1 == ref)).map Prod.snd).getD 0
  let common := [("address", toJson address), ("index", num index)]
  match items[index]? with
  | none => return (obj (("kind", str "stream-success") :: common), cursors)
  | some (.error _, _) => return (obj (("kind", str "stream-failure") :: common), cursors)
  | some (.ok _, _) =>
      return (obj (("kind", str "stream-items") :: ("count", num 1) :: common),
        (ref, index + 1) :: cursors.filter (fun pair => pair.1 != ref))

def automaticSchedule (w : Work) (initial : A) : Except String (List Json) := do
  let mut state := initial
  let mut cursors := []
  let mut schedule := []
  for _ in List.range 4096 do
    if state.stopped || (state.queue.rootGroups.isEmpty && state.queue.rootStreams.isEmpty && state.queue.groupFailureEvents.isEmpty) then return schedule
    let (json, nextCursors) ← nextAutomaticInput w state cursors
    let input ← event w json
    if !state.queue.acceptsBatch [input] then throw "automatic schedule is not admissible"
    state := (state.handleGraphEvents [input]).state
    cursors := nextCursors
    schedule := schedule ++ [arr [json]]
  throw "model did not terminate in 4096 steps"

def rawEvent : WorkQueueEvent → Json
  | .groupValues g vs => obj [("kind", str "GROUP_VALUES"), ("group", num g.ref), ("values", arr (vs.map groupValue))]
  | .groupSuccess g gs ss => obj [("kind", str "GROUP_SUCCESS"), ("group", num g.ref), ("newGroups", arr (gs.map (num ∘ DeliveryNode.ref))), ("newStreams", arr (ss.map (num ∘ DeliveryNode.ref)))]
  | .groupFailure g n => obj [("kind", str "GROUP_FAILURE"), ("group", num g.ref), ("errors", num n)]
  | .streamValues s vs gs ss => obj [("kind", str "STREAM_VALUES"), ("stream", num s.ref), ("values", arr (vs.map itemValue)), ("newGroups", arr (gs.map (num ∘ DeliveryNode.ref))), ("newStreams", arr (ss.map (num ∘ DeliveryNode.ref)))]
  | .streamSuccess s => obj [("kind", str "STREAM_SUCCESS"), ("stream", num s.ref)]
  | .streamFailure s n => obj [("kind", str "STREAM_FAILURE"), ("stream", num s.ref), ("errors", num n)]
  | .workQueueTermination => obj [("kind", str "WORK_QUEUE_TERMINATION")]

def effect : AlternativeWorkQueue.HostEffect → Json
  | .adopt o => obj [("kind", str "adopt"), ("occurrence", occurrence o)]
  | .abortTask o => obj [("kind", str "abortTask"), ("occurrence", occurrence o)]
  | .acknowledge s => obj [("kind", str "acknowledge"), ("stream", num s)]
  | .cancelSource s => obj [("kind", str "cancelSource"), ("stream", num s)]
  | .abortStreamQueue s => obj [("kind", str "abortStreamQueue"), ("stream", num s)]
  | .responseFinished => obj [("kind", str "responseFinished")]

def stateFacts (s : A) : Json := obj [
  ("stopped", toJson s.stopped), ("rootGroups", toJson s.queue.rootGroups), ("rootStreams", toJson s.queue.rootStreams),
  ("groups", arr (s.queue.groupNodes.map fun n => obj [
    ("key", num n.group.node.ref), ("children", toJson n.childGroups), ("tasks", arr (n.tasks.map occurrence)),
    ("pending", num n.pending)])),
  ("tasks", arr (s.queue.taskNodes.map fun n => obj [
    ("occurrence", occurrence n.task.occurrence), ("value", n.value.map groupValue |>.getD .null), ("childStreams", toJson n.childStreams), ("rejection", n.rejection.map num |>.getD .null)])),
  ("groupFailures", arr (s.queue.groupFailureEvents.map fun f => obj [("group", num f.group.ref), ("errors", num f.errors)]))]

def run (j : Json) : Except String Json := do
  if (← j.getObjValAs? Nat "version") != 3 then throw "unsupported native oracle protocol"
  let s ← schema (← get j "schema")
  let selections ← (← (← get j "selections").getArr?).toList.mapM selection
  let variables ← (← entries (← get j "variables")).mapM fun (k, v) => do pure (k, ← constant v)
  let executionPolicy ← match (get j "executionPolicy").toOption with
    | none => pure "eager"
    | some policy => policy.getStr?
  let root := value (← get j "root")
  let action ← match executionPolicy with
    | "eager" => pure (executeRootSelectionSetCore s resolvers variables 256 "Query" root selections)
    | "sequential" =>
        pure (AlternativeWorkQueue.NativeRefinement.ScheduledExecution.executeRootSelectionSetCore
          s resolvers variables 256 "Query" root selections)
    | _ => throw "unknown execution policy"
  let (completed, supply) := action.run 0
  let response := selectionSetResultToResponse completed.result
  let (first, cursor) := AlternativeWorkQueue.ResponseStreamCursor.buildResponse response completed.work
  let mut state := cursor.queue
  let mut publisher := cursor.publisher
  let mut steps := []
  let mut admission := []
  let initial := stateFacts state
  let schedule ← if (j.getObjValAs? Bool "autoSchedule").toOption == some true then
      automaticSchedule completed.work state
    else pure (← (← get j "schedule").getArr?).toList
  for batch in schedule do
    let (next, events, effects) ← if (batch.getObjValAs? Bool "cancel").toOption == some true then do
        admission := admission ++ [Json.null]
        let (next, effects) := state.cancel
        pure (next, [], effects)
      else do
        let inputs ← (← batch.getArr?).toList.mapM (event completed.work)
        admission := admission ++ [if state.stopped || inputs.isEmpty then Json.null else toJson (state.queue.acceptsBatch inputs)]
        let reduced := state.handleGraphEvents inputs
        pure (reduced.state, reduced.workQueueEvents, reduced.effects)
    state := next
    let mut wire := Json.null
    if !events.isEmpty then
      let (u, p) := publisher.handleBatch events
      publisher := p
      wire := update u
    steps := steps ++ [obj [("state", stateFacts state), ("events", arr (events.map rawEvent)), ("effects", arr (effects.map effect)), ("wire", wire)]]
  pure (obj [("executionPolicy", str executionPolicy), ("finalSupply", num supply),
    ("work", work completed.work), ("response", obj [("data", responseValue response.data), ("errors", num response.errors)]),
    ("initialPending", arr (first.pending.map pending)),
    ("schedule", arr schedule), ("initial", initial), ("steps", arr steps), ("admission", arr admission)])

partial def serve : IO Unit := do
  let stdin ← IO.getStdin
  let stdout ← IO.getStdout
  let line ← stdin.getLine
  if line.isEmpty || line.trimAscii.toString == "quit" then return
  let output := match (do
    let j ← Json.parse line
    pure (obj [("id", ← get j "id"), ("result", ← run j)])) with
    | .ok output => output
    | .error message => obj [("error", str message)]
  stdout.putStrLn output.compress
  stdout.flush
  serve
end NativeOracle

def main : IO Unit := NativeOracle.serve
