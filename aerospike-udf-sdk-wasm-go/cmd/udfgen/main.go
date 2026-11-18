// Copyright 2015-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

// udfgen writes the exports of a WASM UDF module from directive comments, the Go face of the Rust SDK's
// export!, stream! and event! macros. Run it with go generate:
//
//	//go:generate go run github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go/cmd/udfgen
//
// It reads the package in the current directory (or the one named as its argument) and writes
// udf_exports.go beside it, with one //export per UDF or handler that decodes the arguments into the
// function's parameters and delivers its return, and one __udf_sig_<name> export per name. The directives
// go in the doc comment of the declaration:
//
//	//udf:function <name> [params="(...)"]         on a func: a record UDF
//	//udf:on xdr-merge <name>                      on a func: an xdr-merge handler
//	//udf:stream <name> [params="(...)"] [end="(...)"]   on a `var x T`: a stream UDF; T has Record and End
//
// A typed function's parameters are int64, int, int32, float64, bool, string, []byte, udf.Value, their
// pointer forms for an optional argument, or []udf.Value last for the rest; it returns nothing, an error, a
// value, or a value and an error, the value being any of those types or *udf.Map / *udf.List. A handler
// takes the two records and the conflict and returns a udf.Resolution and an error. A function declared
// (args, argc int32) int32 is the raw form, exported as it is, with params giving the signature text.
//
// A udf:function, and a stream's Record method, may take the record first, as a *udf.Record, in the typed
// form and the raw one alike. The export passes it the record of the call. It is not an argument: it is
// left out of the arity check and of the signature text, so adding or removing it changes nothing a
// client sees. A stream's End takes none, since no record is in hand when the stream ends, and
// neither does a handler: an event reads its records as *udf.EventRecord.
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const sdkPath = "github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go"

// The one event kind, and what a typed handler of it takes and returns, with the SDK alias as "udf".
const (
	mergeEvent  = "xdr-merge"
	mergeAnswer = "udf.Resolution"
)

var mergeParams = []string{"*udf.EventRecord", "*udf.EventRecord", "*udf.Conflict"}

type role int

const (
	roleFunction role = iota
	roleRecord
	roleEnd
	roleHandler
)

type export struct {
	role     role
	name     string
	params   string // the signature's parameter text, raw form
	fn       *ast.FuncDecl
	receiver string // the stream variable, for a method
	pos      token.Pos
}

type generator struct {
	fset   *token.FileSet
	alias  string // the SDK import alias in the package
	pkg    string
	dir    string
	errors []string
}

func main() {
	dir := "."
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}
	g := &generator{fset: token.NewFileSet(), dir: dir, alias: "udf"}
	pkgs, err := parser.ParseDir(g.fset, dir, func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go") && fi.Name() != "udf_exports.go"
	}, parser.ParseComments)
	if err != nil {
		fail("%v", err)
	}
	var pkg *ast.Package
	for _, p := range pkgs {
		pkg = p
	}
	if pkg == nil {
		fail("%s: no Go package", dir)
	}
	g.pkg = pkg.Name
	exports := g.collect(pkg)
	if len(g.errors) > 0 {
		for _, e := range g.errors {
			fmt.Fprintln(os.Stderr, e)
		}
		os.Exit(1)
	}
	if len(exports) == 0 {
		fail("%s: no udf: directives", dir)
	}
	out := g.generate(exports)
	if len(g.errors) > 0 {
		for _, e := range g.errors {
			fmt.Fprintln(os.Stderr, e)
		}
		os.Exit(1)
	}
	path := filepath.Join(dir, "udf_exports.go")
	if err := os.WriteFile(path, []byte(out), 0o644); err != nil {
		fail("%v", err)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "udfgen: "+format+"\n", args...)
	os.Exit(1)
}

func (g *generator) errorf(pos token.Pos, format string, args ...any) {
	g.errors = append(g.errors, fmt.Sprintf("%s: %s", g.fset.Position(pos), fmt.Sprintf(format, args...)))
}

//==========================================================
// Reading the directives.
//

// directive is one `udf:` line of a doc comment: its words and its key="value" options.
type directive struct {
	words []string
	opts  map[string]string
	pos   token.Pos
}

func directives(doc *ast.CommentGroup) []directive {
	if doc == nil {
		return nil
	}
	var out []directive
	for _, c := range doc.List {
		text := strings.TrimPrefix(c.Text, "//")
		text = strings.TrimSpace(text)
		if !strings.HasPrefix(text, "udf:") {
			continue
		}
		d := directive{opts: map[string]string{}, pos: c.Pos()}
		rest := strings.TrimPrefix(text, "udf:")
		for len(rest) > 0 {
			rest = strings.TrimLeft(rest, " \t")
			if rest == "" {
				break
			}
			if eq := strings.Index(rest, "=\""); eq > 0 && !strings.ContainsAny(rest[:eq], " \t") {
				key := rest[:eq]
				rest = rest[eq+2:]
				end := strings.Index(rest, "\"")
				if end < 0 {
					d.opts[key] = rest
					rest = ""
				} else {
					d.opts[key] = rest[:end]
					rest = rest[end+1:]
				}
				continue
			}
			sp := strings.IndexAny(rest, " \t")
			if sp < 0 {
				d.words = append(d.words, rest)
				rest = ""
			} else {
				d.words = append(d.words, rest[:sp])
				rest = rest[sp:]
			}
		}
		out = append(out, d)
	}
	return out
}

func (g *generator) collect(pkg *ast.Package) []*export {
	var exports []*export
	methods := map[string]map[string]*ast.FuncDecl{} // type name -> method name -> decl
	names := map[string]token.Pos{}

	// The SDK's alias, and every method, first.
	for _, f := range pkg.Files {
		for _, imp := range f.Imports {
			if strings.Trim(imp.Path.Value, "\"") == sdkPath && imp.Name != nil {
				g.alias = imp.Name.Name
			}
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) == 0 {
				continue
			}
			t := fn.Recv.List[0].Type
			if star, ok := t.(*ast.StarExpr); ok {
				t = star.X
			}
			if id, ok := t.(*ast.Ident); ok {
				if methods[id.Name] == nil {
					methods[id.Name] = map[string]*ast.FuncDecl{}
				}
				methods[id.Name][fn.Name.Name] = fn
			}
		}
	}

	add := func(e *export) {
		key := e.name
		if e.role == roleRecord {
			key = "record " + e.name
		} else if e.role == roleEnd {
			key = "end " + e.name
		}
		if prev, dup := names[key]; dup {
			g.errorf(e.pos, "the name %s is exported twice (also at %s)", e.name, g.fset.Position(prev))
			return
		}
		names[key] = e.pos
		exports = append(exports, e)
	}

	for _, f := range pkg.Files {
		for _, d := range f.Decls {
			switch decl := d.(type) {
			case *ast.FuncDecl:
				for _, dir := range directives(decl.Doc) {
					if decl.Recv != nil {
						g.errorf(dir.pos, "a udf: directive on a method; put //udf:stream on the variable")
						continue
					}
					switch dir.words[0] {
					case "function":
						if len(dir.words) != 2 {
							g.errorf(dir.pos, "udf:function <name>")
							continue
						}
						add(&export{role: roleFunction, name: dir.words[1], params: dir.opts["params"], fn: decl, pos: dir.pos})
					case "on":
						if len(dir.words) != 3 {
							g.errorf(dir.pos, "udf:on %s <name>", mergeEvent)
							continue
						}
						if dir.words[1] != mergeEvent {
							g.errorf(dir.pos, "unknown event kind %s: the one event is %s", dir.words[1], mergeEvent)
							continue
						}
						add(&export{role: roleHandler, name: dir.words[2], fn: decl, pos: dir.pos})
					default:
						g.errorf(dir.pos, "unknown directive udf:%s on a function", dir.words[0])
					}
				}
			case *ast.GenDecl:
				if decl.Tok != token.VAR {
					continue
				}
				for _, spec := range decl.Specs {
					vs := spec.(*ast.ValueSpec)
					doc := vs.Doc
					if doc == nil {
						doc = decl.Doc
					}
					for _, dir := range directives(doc) {
						if dir.words[0] != "stream" {
							g.errorf(dir.pos, "unknown directive udf:%s on a variable", dir.words[0])
							continue
						}
						if len(dir.words) != 2 || len(vs.Names) != 1 || vs.Type == nil {
							g.errorf(dir.pos, "udf:stream <name> goes on `var x T`")
							continue
						}
						typ := vs.Type
						if star, ok := typ.(*ast.StarExpr); ok {
							typ = star.X
						}
						id, ok := typ.(*ast.Ident)
						if !ok || methods[id.Name] == nil || methods[id.Name]["Record"] == nil || methods[id.Name]["End"] == nil {
							g.errorf(dir.pos, "a stream's type has a Record method and an End method")
							continue
						}
						v := vs.Names[0].Name
						add(&export{role: roleRecord, name: dir.words[1], params: dir.opts["params"], fn: methods[id.Name]["Record"], receiver: v, pos: dir.pos})
						add(&export{role: roleEnd, name: dir.words[1], params: dir.opts["end"], fn: methods[id.Name]["End"], receiver: v, pos: dir.pos})
					}
				}
			}
		}
	}
	sort.SliceStable(exports, func(i, j int) bool { return exports[i].pos < exports[j].pos })
	return exports
}

//==========================================================
// Types.
//

// typeText is a type expression as text, with the SDK's alias normalised to "udf".
func (g *generator) typeText(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return "*" + g.typeText(t.X)
	case *ast.SelectorExpr:
		if id, ok := t.X.(*ast.Ident); ok && id.Name == g.alias {
			return "udf." + t.Sel.Name
		}
		return g.typeText(t.X) + "." + t.Sel.Name
	case *ast.ArrayType:
		if t.Len == nil {
			return "[]" + g.typeText(t.Elt)
		}
	}
	return "?"
}

// params is every parameter as (name, type text), one per name.
func (g *generator) params(fn *ast.FuncDecl) (names, types []string) {
	for _, f := range fn.Type.Params.List {
		t := g.typeText(f.Type)
		if len(f.Names) == 0 {
			names = append(names, "")
			types = append(types, t)
		}
		for _, n := range f.Names {
			names = append(names, n.Name)
			types = append(types, t)
		}
	}
	return
}

func (g *generator) results(fn *ast.FuncDecl) []string {
	if fn.Type.Results == nil {
		return nil
	}
	var out []string
	for _, f := range fn.Type.Results.List {
		t := g.typeText(f.Type)
		if len(f.Names) == 0 {
			out = append(out, t)
		}
		for range f.Names {
			out = append(out, t)
		}
	}
	return out
}

// recordType is the parameter type that asks for the record of the call.
const recordType = "*udf.Record"

// arguments is a function's parameters without the leading record, and whether it declared one. The
// record is handed over by the export, so nothing that concerns the client's arguments counts it.
func (g *generator) arguments(fn *ast.FuncDecl) (names, types []string, record bool) {
	names, types = g.params(fn)
	if len(types) > 0 && types[0] == recordType {
		return names[1:], types[1:], true
	}
	return names, types, false
}

// raw reports whether a function is in the raw form, (args, argc int32) int32, after the record if it
// takes one.
func (g *generator) raw(fn *ast.FuncDecl) bool {
	_, types, _ := g.arguments(fn)
	res := g.results(fn)
	return len(types) == 2 && types[0] == "int32" && types[1] == "int32" && len(res) == 1 && res[0] == "int32"
}

// decoded is one parameter's decode call and signature text.
type decoded struct {
	call string
	sig  string
	rest bool
}

// decode is the decoder of a parameter of type t, or nil when the type has no form.
func (g *generator) decode(name, t string, i, count int, fn, args string) *decoded {
	call := func(f string) string {
		return fmt.Sprintf("udf.%s(%s, n, %d, %q, %q)", f, args, i, fn, name)
	}
	sig := func(optional bool, kind string) string {
		if optional {
			return name + "?: " + kind
		}
		return name + ": " + kind
	}
	switch t {
	case "int64":
		return &decoded{call("ArgInt64"), sig(false, "int"), false}
	case "int":
		return &decoded{call("ArgInt"), sig(false, "int"), false}
	case "int32":
		return &decoded{call("ArgInt32"), sig(false, "int"), false}
	case "float64":
		return &decoded{call("ArgFloat64"), sig(false, "double"), false}
	case "bool":
		return &decoded{call("ArgBool"), sig(false, "bool"), false}
	case "string":
		return &decoded{call("ArgString"), sig(false, "str"), false}
	case "[]byte":
		return &decoded{call("ArgBytes"), sig(false, "bytes"), false}
	case "udf.Value":
		return &decoded{call("ArgValue"), sig(false, "any"), false}
	case "*int64":
		return &decoded{call("OptInt64"), sig(true, "int"), false}
	case "*float64":
		return &decoded{call("OptFloat64"), sig(true, "double"), false}
	case "*bool":
		return &decoded{call("OptBool"), sig(true, "bool"), false}
	case "*string":
		return &decoded{call("OptString"), sig(true, "str"), false}
	case "*udf.Value":
		return &decoded{call("OptValue"), sig(true, "any"), false}
	case "[]udf.Value":
		if i != count-1 {
			return nil
		}
		return &decoded{fmt.Sprintf("udf.ArgRest(%s, n, %d)", args, i), name + ": any...", true}
	}
	return nil
}

// deliver is the statement that sets a result of type t from the expression r, "" when t has no form.
func deliver(t, r string) string {
	switch t {
	case "int64":
		return "udf.ResultInt(" + r + ")"
	case "int", "int32":
		return "udf.ResultInt(int64(" + r + "))"
	case "float64":
		return "udf.ResultDouble(" + r + ")"
	case "bool":
		return "udf.ResultBool(" + r + ")"
	case "string":
		return "udf.ResultStr(" + r + ")"
	case "[]byte":
		return "udf.ResultBlob(" + r + ")"
	case "udf.Value":
		return "if err := udf.ResultOf(" + r + "); err != nil {\n\t\treturn udf.FailErr(err)\n\t}"
	case "*udf.Map":
		return "if err := udf.ResultMap(" + r + "); err != nil {\n\t\treturn udf.FailErr(err)\n\t}"
	case "*udf.List":
		return "if err := udf.ResultList(" + r + "); err != nil {\n\t\treturn udf.FailErr(err)\n\t}"
	}
	return ""
}

//==========================================================
// Writing the file.
//

func (g *generator) generate(exports []*export) string {
	var b strings.Builder
	fmt.Fprintf(&b, "// Code generated by udfgen; DO NOT EDIT.\n\npackage %s\n\nimport udf %q\n", g.pkg, sdkPath)
	streams := map[string][2]string{}
	var order []string

	for _, e := range exports {
		id := identifier(e.name)
		var params string
		if e.role == roleHandler {
			g.handler(&b, e, id)
		} else {
			params = g.function(&b, e, id)
		}
		switch e.role {
		case roleFunction:
			signature(&b, e.name, id, "fn "+e.name+params+" -> value")
		case roleHandler:
			signatureExpr(&b, e.name, id, fmt.Sprintf("udf.XdrMergeSignature(%q)", e.name))
		default:
			halves, seen := streams[e.name]
			if !seen {
				order = append(order, e.name)
			}
			if e.role == roleRecord {
				halves[0] = params
			} else {
				halves[1] = params
			}
			streams[e.name] = halves
		}
	}
	for _, name := range order {
		h := streams[name]
		signature(&b, name, identifier(name), "stream "+name+" { record"+h[0]+" end"+h[1]+" }")
	}
	return b.String()
}

func (g *generator) call(e *export) string {
	if e.receiver != "" {
		return e.receiver + "." + e.fn.Name.Name
	}
	return e.fn.Name.Name
}

// function writes a record or stream UDF's export and returns the signature's parameter text.
func (g *generator) function(b *strings.Builder, e *export, id string) string {
	exportName := e.name
	switch e.role {
	case roleRecord:
		exportName += "_record"
	case roleEnd:
		exportName += "_end"
	}
	prefix := "udfExport"
	if e.role == roleRecord {
		prefix = "udfRecord"
	} else if e.role == roleEnd {
		prefix = "udfEnd"
	}
	fmt.Fprintf(b, "\n//export %s\nfunc %s_%s(args, argc int32) int32 {\n", exportName, prefix, id)

	names, types, record := g.arguments(e.fn)
	if record && e.role == roleEnd {
		g.errorf(e.fn.Pos(), "a stream's End takes no record: none is in hand when the stream ends")
		return ""
	}

	if g.raw(e.fn) {
		if record {
			fmt.Fprintf(b, "\treturn %s(udf.CurrentRecord(), args, argc)\n}\n", g.call(e))
		} else {
			fmt.Fprintf(b, "\treturn %s(args, argc)\n}\n", g.call(e))
		}
		if e.params == "" {
			return "()"
		}
		return e.params
	}
	if e.params != "" {
		g.errorf(e.pos, "params is for the raw form (args, argc int32) int32; a typed function's signature is derived")
	}

	var decs []*decoded
	rest := false
	for i := range names {
		if types[i] == recordType {
			g.errorf(e.fn.Pos(), "parameter %s: the record is the first parameter, and there is one", names[i])
			return ""
		}
		d := g.decode(names[i], types[i], i, len(names), e.name, "a[:]")
		if d == nil {
			g.errorf(e.fn.Pos(), "parameter %s: %s is not a type a udf:function takes", names[i], types[i])
			return ""
		}
		decs = append(decs, d)
		rest = rest || d.rest
	}
	capacity := len(names)
	if rest {
		capacity = 128
	} else if capacity == 0 {
		capacity = 1
	}
	if len(names) > 0 {
		fmt.Fprintf(b, "\tvar a [%d]udf.Value\n\tn := udf.Args(args, argc, a[:])\n", capacity)
	}
	if !rest {
		fmt.Fprintf(b, "\tif err := udf.Arity(%q, %d, argc); err != nil {\n\t\treturn udf.FailErr(err)\n\t}\n", e.name, len(names))
	}
	// The record goes in ahead of the decoded arguments; the indexes above never counted it.
	var actuals []string
	if record {
		actuals = append(actuals, "udf.CurrentRecord()")
	}
	var sig []string
	for i, d := range decs {
		p := fmt.Sprintf("p%d", i)
		if d.rest {
			fmt.Fprintf(b, "\t%s := %s\n", p, d.call)
		} else {
			fmt.Fprintf(b, "\t%s, err := %s\n\tif err != nil {\n\t\treturn udf.FailErr(err)\n\t}\n", p, d.call)
			if i > 0 && !decs[i-1].rest {
				// err is reused; Go wants at least one new variable on the left, which p is.
			}
		}
		actuals = append(actuals, p)
		sig = append(sig, d.sig)
	}
	call := g.call(e) + "(" + strings.Join(actuals, ", ") + ")"
	res := g.results(e.fn)

	// What the function returns: nothing, error, T, or (T, error).
	switch {
	case e.role == roleRecord:
		switch {
		case len(res) == 0:
			fmt.Fprintf(b, "\t%s\n", call)
		case len(res) == 1 && res[0] == "error":
			fmt.Fprintf(b, "\tif err := %s; err != nil {\n\t\treturn udf.FailErr(err)\n\t}\n", call)
		default:
			g.errorf(e.fn.Pos(), "a stream's Record returns nothing or an error (or int32 in the raw form)")
			return ""
		}
	case len(res) == 0:
		fmt.Fprintf(b, "\t%s\n\tudf.ResultNil()\n", call)
	case len(res) == 1 && res[0] == "error":
		fmt.Fprintf(b, "\tif err := %s; err != nil {\n\t\treturn udf.FailErr(err)\n\t}\n\tudf.ResultNil()\n", call)
	case len(res) == 1 && deliver(res[0], "r") != "":
		fmt.Fprintf(b, "\tr := %s\n\t%s\n", call, deliver(res[0], "r"))
	case len(res) == 2 && res[1] == "error" && deliver(res[0], "r") != "":
		fmt.Fprintf(b, "\tr, err := %s\n\tif err != nil {\n\t\treturn udf.FailErr(err)\n\t}\n\t%s\n", call, deliver(res[0], "r"))
	default:
		g.errorf(e.fn.Pos(), "a udf:function returns nothing, an error, a value, or a value and an error; the value is int64, int, int32, float64, bool, string, []byte, udf.Value, *udf.Map or *udf.List")
		return ""
	}
	fmt.Fprintf(b, "\treturn 0\n}\n")
	return "(" + strings.Join(sig, ", ") + ")"
}

// handler writes an xdr-merge handler's export.
func (g *generator) handler(b *strings.Builder, e *export, id string) {
	fmt.Fprintf(b, "\n//export %s\nfunc udfExport_%s(args, argc int32) int32 {\n", e.name, id)
	if g.raw(e.fn) {
		if _, _, record := g.arguments(e.fn); record {
			g.errorf(e.fn.Pos(), "a udf:on %s handler takes no *udf.Record: an event reads its records as *udf.EventRecord", mergeEvent)
			return
		}
		fmt.Fprintf(b, "\treturn %s(args, argc)\n}\n", g.call(e))
		return
	}

	_, types := g.params(e.fn)
	res := g.results(e.fn)
	ok := len(types) == len(mergeParams) && len(res) == 2 && res[0] == mergeAnswer && res[1] == "error"
	for i := 0; ok && i < len(types); i++ {
		ok = types[i] == mergeParams[i]
	}
	if !ok {
		g.errorf(e.fn.Pos(), "a udf:on %s handler is func(%s) (%s, error), or the raw func(args, argc int32) int32",
			mergeEvent, strings.Join(mergeParams, ", "), mergeAnswer)
		return
	}

	fmt.Fprintf(b, "\tvar c udf.Conflict\n\tif !udf.DecodeConflict(args, argc, &c) {\n\t\treturn udf.Fail(%q)\n\t}\n",
		e.name+": not an "+mergeEvent+" event")
	fmt.Fprintf(b, "\tr, err := %s(&c.Local, &c.Remote, &c)\n\tif err != nil {\n\t\treturn udf.FailErr(err)\n\t}\n\treturn r.Deliver()\n}\n", g.call(e))
}

func signature(b *strings.Builder, name, id, text string) {
	signatureExpr(b, name, id, fmt.Sprintf("[]byte(%q)", text+"\x00"))
}

func signatureExpr(b *strings.Builder, name, id, expr string) {
	fmt.Fprintf(b, "\nvar udfSig_%s = %s\n\n//export __udf_sig_%s\nfunc udfSigFn_%s() int32 { return udf.Signature(&udfSig_%s) }\n", id, expr, name, id, id)
}

func identifier(name string) string {
	var b strings.Builder
	for _, r := range name {
		if r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}
