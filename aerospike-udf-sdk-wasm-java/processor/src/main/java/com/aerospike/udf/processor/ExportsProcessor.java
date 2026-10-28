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

package com.aerospike.udf.processor;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;

/**
 * Generates the exports of a module from its annotations: for every class carrying {@code @Function} or
 * {@code @On.*} methods, or annotated {@code @Stream} itself, a class {@code <Class>Exports} in the same
 * package with one TeaVM {@code @Export} method per UDF or handler, which decodes the arguments into the
 * method's parameters and delivers its return, one {@code __udf_sig_<name>} export per name, and the
 * module's {@code main}. A class whose exported methods are instance methods gets one instance, built
 * once, in the generated class. The annotations are matched by name, so the processor depends on nothing
 * but the JDK and never sees the SDK; the generated code refers to the SDK by qualified name.
 */
public final class ExportsProcessor extends AbstractProcessor {
    private static final String SDK = "com.aerospike.udf.";
    private static final String FUNCTION = SDK + "Function";
    private static final String STREAM = SDK + "Stream";
    private static final String NULLABLE = SDK + "Nullable";
    private static final String ON = SDK + "On.";

    private static final String UDF = SDK + "Udf";
    private static final String VALUE = SDK + "Value";
    private static final String UDF_MAP = SDK + "UdfMap";
    private static final String UDF_LIST = SDK + "UdfList";
    private static final String PARAMS = SDK + "Params";
    private static final String EVENTS = SDK + "Events";
    private static final String CONFLICT = SDK + "Conflict";
    private static final String EVENT_RECORD = SDK + "EventRecord";
    private static final String UDF_EXCEPTION = SDK + "UdfException";

    /** One event kind: its annotation, the server's name for it, the handler's parameters and answer. */
    private static final class Kind {
        final String annotation;
        final String event;
        final String constant;
        final String[] params;
        final String answer;
        final String[] records;

        Kind(String annotation, String event, String constant, String[] params, String answer, String... records) {
            this.annotation = annotation;
            this.event = event;
            this.constant = constant;
            this.params = params;
            this.answer = answer;
            this.records = records;
        }
    }

    private static final Map<String, Kind> KINDS = new HashMap<>();

    static {
        String[] write = { EVENT_RECORD, EVENT_RECORD, EVENTS + ".WriteInfo" };
        String[] delete = { EVENT_RECORD, EVENTS + ".DeleteInfo" };
        String[] expire = { EVENT_RECORD, EVENTS + ".ExpireInfo" };
        String[] truncate = { EVENTS + ".TruncateInfo" };

        kind("BeforeWrite", "before-write", "PARAMS_BEFORE_WRITE", write, SDK + "Decision", "before", "after");
        kind("AfterWrite", "after-write", "PARAMS_AFTER_WRITE", write, null, "before", "after");
        kind("BeforeDelete", "before-delete", "PARAMS_BEFORE_DELETE", delete, SDK + "Gate", "record");
        kind("AfterDelete", "after-delete", "PARAMS_AFTER_DELETE", delete, null, "record");
        kind("BeforeExpire", "before-expire", "PARAMS_BEFORE_EXPIRE", expire, SDK + "Expiry", "record");
        kind("BeforeEvict", "before-evict", "PARAMS_BEFORE_EVICT", expire, SDK + "Eviction", "record");
        kind("AfterRead", "after-read", "PARAMS_AFTER_READ", new String[] { EVENT_RECORD, EVENTS + ".ReadInfo" },
                SDK + "ReadResult", "record");
        kind("BeforeTruncate", "before-truncate", "PARAMS_BEFORE_TRUNCATE", truncate, SDK + "Gate");
        kind("AfterTruncate", "after-truncate", "PARAMS_AFTER_TRUNCATE", truncate, null);
        kind("BeforeShip", "before-ship", "PARAMS_BEFORE_SHIP", new String[] { EVENT_RECORD, EVENTS + ".ShipInfo" },
                SDK + "ShipDecision", "record");
        kind("PartitionTransfer", "partition-transfer", "PARAMS_PARTITION_TRANSFER",
                new String[] { EVENTS + ".TransferInfo" }, null);
        kind("ClusterChange", "cluster-change", "PARAMS_CLUSTER_CHANGE", new String[] { EVENTS + ".ClusterInfo" }, null);
        kind("NodeLifecycle", "node-lifecycle", "PARAMS_NODE_LIFECYCLE", new String[] { EVENTS + ".LifecycleInfo" }, null);
        kind("Timer", "timer", "PARAMS_TIMER", new String[] { EVENTS + ".TimerInfo" }, null);
        kind("XdrMerge", "xdr-merge", null, new String[] { EVENT_RECORD, EVENT_RECORD, CONFLICT }, SDK + "Resolution");
    }

    private static void kind(String annotation, String event, String constant, String[] params, String answer,
            String... records) {
        KINDS.put(ON + annotation, new Kind(annotation, event, constant, params, answer, records));
    }

    private enum Role { FUNCTION, RECORD, END, HANDLER }

    /** What one method exports. */
    private static final class Export {
        final ExecutableElement method;
        final Role role;
        final String name;
        final String params;
        final Kind kind;

        Export(ExecutableElement method, Role role, String name, String params, Kind kind) {
            this.method = method;
            this.role = role;
            this.name = name;
            this.params = params;
            this.kind = kind;
        }

        boolean raw() {
            List<? extends VariableElement> p = method.getParameters();

            return p.size() == 2 && p.get(0).asType().getKind() == TypeKind.INT
                    && p.get(1).asType().getKind() == TypeKind.INT
                    && method.getReturnType().getKind() == TypeKind.INT;
        }

        boolean instance() {
            return !method.getModifiers().contains(Modifier.STATIC);
        }

        String annotation() {
            switch (role) {
            case FUNCTION:
                return "Function";
            case HANDLER:
                return "On." + kind.annotation;
            default:
                return "Stream";
            }
        }
    }

    /** A parameter decoded: the expression that yields it and its text in the signature. */
    private static final class Param {
        final String expr;
        final String sig;
        final boolean rest;

        Param(String expr, String sig, boolean rest) {
            this.expr = expr;
            this.sig = sig;
            this.rest = rest;
        }
    }

    private boolean failed;

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        Set<String> names = new HashSet<>();

        names.add(FUNCTION);
        names.add(STREAM);
        names.addAll(KINDS.keySet());

        return names;
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment env) {
        Map<TypeElement, Set<Element>> byClass = new LinkedHashMap<>();

        for (TypeElement a : annotations) {
            for (Element e : env.getElementsAnnotatedWith(a)) {
                if (e.getKind() == ElementKind.METHOD) {
                    TypeElement owner = (TypeElement) e.getEnclosingElement();

                    byClass.computeIfAbsent(owner, k -> new LinkedHashSet<>()).add(e);
                }
                else if (e.getKind() == ElementKind.CLASS) {
                    byClass.computeIfAbsent((TypeElement) e, k -> new LinkedHashSet<>()).add(e);
                }
            }
        }

        for (Map.Entry<TypeElement, Set<Element>> entry : byClass.entrySet()) {
            failed = false;

            List<Export> exports = collect(entry.getKey(), entry.getValue());

            if (!failed) {
                generate(entry.getKey(), exports);
            }
        }

        return true;
    }

    //==========================================================
    // Reading the annotations.
    //

    private List<Export> collect(TypeElement owner, Set<Element> elements) {
        if (owner.getModifiers().contains(Modifier.PRIVATE)) {
            error(owner, "a class with exported methods may not be private");
        }

        if (owner.getNestingKind().isNested() && !owner.getModifiers().contains(Modifier.STATIC)) {
            error(owner, "a nested class with exported methods must be static");
        }

        List<Export> exports = new ArrayList<>();
        Set<String> names = new HashSet<>();

        // The class's own @Stream: its record and end methods.
        AnnotationMirror stream = annotation(owner, STREAM);

        if (stream != null) {
            String name = string(stream, "value");
            ExecutableElement record = method(owner, "record");
            ExecutableElement end = method(owner, "end");

            if (name.isEmpty()) {
                error(owner, "@Stream names the stream");
            }

            if (record == null || end == null) {
                error(owner, "a @Stream class has a record method and an end method");
            }
            else {
                exports.add(new Export(record, Role.RECORD, name, string(stream, "params"), null));
                exports.add(new Export(end, Role.END, name, string(stream, "endParams"), null));
                names.add(name);
            }
        }

        for (Element e : elements) {
            if (e.getKind() != ElementKind.METHOD) {
                continue;
            }

            ExecutableElement method = (ExecutableElement) e;
            Export export = null;

            for (AnnotationMirror mirror : method.getAnnotationMirrors()) {
                String type = qualifiedName(mirror.getAnnotationType());

                if (!type.equals(FUNCTION) && !KINDS.containsKey(type)) {
                    continue;
                }

                if (export != null) {
                    error(method, "a method exports one thing: it carries both @" + export.annotation()
                            + " and @" + simple(type));
                    continue;
                }

                String name = string(mirror, "value");

                if (name.isEmpty()) {
                    name = method.getSimpleName().toString();
                }

                export = type.equals(FUNCTION)
                        ? new Export(method, Role.FUNCTION, name, string(mirror, "params"), null)
                        : new Export(method, Role.HANDLER, name, "", KINDS.get(type));
            }

            if (export != null) {
                exports.add(export);

                if (!names.add(export.name)) {
                    error(method, "the name " + export.name + " is exported twice in " + owner.getSimpleName());
                }
            }
        }

        for (Export export : exports) {
            check(owner, export);
        }

        return exports;
    }

    private void check(TypeElement owner, Export e) {
        ExecutableElement method = e.method;

        if (method.getModifiers().contains(Modifier.PRIVATE)) {
            error(method, "an exported method may not be private: the generated class calls it");
        }

        if (!method.getTypeParameters().isEmpty()) {
            error(method, "an exported method may not be generic");
        }

        if (e.instance() && constructor(owner) == null) {
            error(method, "an exported instance method needs a constructor without arguments that "
                    + owner.getSimpleName() + "Exports can call");
        }

        if (!e.params.isEmpty() && !e.raw()) {
            error(method, "params is for the raw form (int args, int argc); the signature of a typed"
                    + " method is derived from its parameters");
        }

        if (e.raw()) {
            return;
        }

        if (e.kind != null) {
            checkHandler(e);
            return;
        }

        List<? extends VariableElement> params = method.getParameters();

        for (int i = 0; i < params.size(); i++) {
            param(params.get(i), i, params.size(), "", "");
        }

        TypeMirror ret = method.getReturnType();

        if (e.role == Role.RECORD) {
            if (ret.getKind() != TypeKind.VOID) {
                error(method, "a typed record method returns void (or int in the raw form)");
            }
        }
        else if (result(ret, "x") == null) {
            error(method, "an exported method returns void, long, int, double, boolean, String, byte[],"
                    + " Value, UdfMap or UdfList (or int, in the raw form)");
        }
    }

    private void checkHandler(Export e) {
        List<? extends VariableElement> params = e.method.getParameters();
        boolean ok = params.size() == e.kind.params.length;

        for (int i = 0; ok && i < params.size(); i++) {
            ok = typeName(params.get(i).asType()).equals(e.kind.params[i]);
        }

        TypeMirror ret = e.method.getReturnType();

        if (e.kind.answer == null) {
            ok &= ret.getKind() == TypeKind.VOID;
        }
        else {
            ok &= ret.getKind() == TypeKind.DECLARED && typeName(ret).equals(e.kind.answer);
        }

        if (!ok) {
            StringBuilder want = new StringBuilder(e.kind.answer == null ? "void " : simple(e.kind.answer) + " ");

            want.append(e.method.getSimpleName()).append('(');

            for (int i = 0; i < e.kind.params.length; i++) {
                want.append(i > 0 ? ", " : "").append(simple(e.kind.params[i]));
            }

            error(e.method, "a @On." + e.kind.annotation + " handler is " + want + "), or the raw int ("
                    + "int args, int argc)");
        }
    }

    //==========================================================
    // Parameters and results.
    //

    /** The decode expression and signature text of a parameter, or null (reported) when it has no form. */
    private Param param(VariableElement p, int i, int count, String fn, String args) {
        TypeMirror t = p.asType();
        String name = p.getSimpleName().toString();
        boolean nullable = annotation(p, NULLABLE) != null;

        String call = PARAMS + "." + (nullable ? "opt" : "") + "%s(" + args + ", n, " + i + ", \"" + fn + "\", \""
                + name + "\")";
        String sig = name + (nullable ? "?: " : ": ");

        switch (t.getKind()) {
        case LONG:
        case INT:
        case DOUBLE:
        case BOOLEAN:
            if (nullable) {
                error(p, "a @Nullable scalar is boxed: Long, Integer, Double or Boolean");
                return null;
            }

            switch (t.getKind()) {
            case LONG:
                return new Param(String.format(call, "int64"), sig + "int", false);
            case INT:
                return new Param(String.format(call, "int32"), sig + "int", false);
            case DOUBLE:
                return new Param(String.format(call, "float64"), sig + "double", false);
            default:
                return new Param(String.format(call, "bool"), sig + "bool", false);
            }
        case ARRAY: {
            TypeMirror component = ((ArrayType) t).getComponentType();

            if (component.getKind() == TypeKind.BYTE) {
                return new Param(String.format(call, nullable ? "Bytes" : "bytes"), sig + "bytes", false);
            }

            if (typeName(component).equals(VALUE)) {
                if (nullable) {
                    error(p, "the rest parameter is never null; it is empty");
                    return null;
                }

                if (i != count - 1) {
                    error(p, "a Value[] parameter takes every remaining argument, so it comes last");
                    return null;
                }

                return new Param(PARAMS + ".rest(" + args + ", n, " + i + ")", name + ": any...", true);
            }

            break;
        }
        case DECLARED: {
            String q = typeName(t);

            switch (q) {
            case "java.lang.Long":
                return new Param(String.format(call, nullable ? "Int64" : "int64"), sig + "int", false);
            case "java.lang.Integer":
                return new Param(String.format(call, nullable ? "Int32" : "int32"), sig + "int", false);
            case "java.lang.Double":
                return new Param(String.format(call, nullable ? "Float64" : "float64"), sig + "double", false);
            case "java.lang.Boolean":
                return new Param(String.format(call, nullable ? "Bool" : "bool"), sig + "bool", false);
            case "java.lang.String":
                return new Param(String.format(call, nullable ? "Str" : "str"), sig + "str", false);
            case VALUE:
                return new Param(String.format(call, nullable ? "Any" : "any"), sig + "any", false);
            default:
                break;
            }

            break;
        }
        default:
            break;
        }

        error(p, "a parameter is long, int, double, boolean, String, byte[], Value, their @Nullable boxed"
                + " forms, or Value[] last; " + t + " is none of these");

        return null;
    }

    /** The statement delivering a call's value as the result, or null when the type has no form. */
    private static String result(TypeMirror ret, String call) {
        switch (ret.getKind()) {
        case VOID:
            return call + ";\n            " + UDF + ".resultNil();";
        case LONG:
        case INT:
            return UDF + ".resultInt(" + call + ");";
        case DOUBLE:
            return UDF + ".resultDouble(" + call + ");";
        case BOOLEAN:
            return UDF + ".resultBool(" + call + ");";
        case ARRAY:
            return ((ArrayType) ret).getComponentType().getKind() == TypeKind.BYTE
                    ? PARAMS + ".resultBytes(" + call + ");" : null;
        case DECLARED: {
            String q = typeName(ret);

            if (q.equals("java.lang.String")) {
                return PARAMS + ".resultStr(" + call + ");";
            }

            if (q.equals(VALUE) || q.equals(UDF_MAP) || q.equals(UDF_LIST)) {
                return PARAMS + ".result(" + call + ");";
            }

            return null;
        }
        default:
            return null;
        }
    }

    //==========================================================
    // Writing the class.
    //

    private void generate(TypeElement owner, List<Export> exports) {
        String pkg = processingEnv.getElementUtils().getPackageOf(owner).getQualifiedName().toString();
        String target = owner.getQualifiedName().toString();
        String simple = nestedName(owner).replace('.', '_') + "Exports";
        StringBuilder out = new StringBuilder();
        boolean instance = false;

        for (Export e : exports) {
            instance |= e.instance();
        }

        out.append("// Generated by aerospike-udf-sdk-wasm-java-processor from ").append(nestedName(owner))
                .append(".java; do not edit.\n");

        if (!pkg.isEmpty()) {
            out.append("package ").append(pkg).append(";\n\n");
        }

        out.append("import org.teavm.interop.Export;\n\n");
        out.append("/** The exports of {@link ").append(nestedName(owner)).append("}: name this class as the")
                .append(" TeaVM mainClass. */\n");
        out.append("public final class ").append(simple).append(" {\n");
        out.append("    private ").append(simple).append("() {\n    }\n");

        if (instance) {
            out.append("\n    /** The one instance the exported instance methods run on. */\n");
            out.append("    private static final ").append(target).append(" INSTANCE = new ").append(target).append("();\n");
        }

        Map<String, String[]> streams = new LinkedHashMap<>();

        for (Export e : exports) {
            String id = identifier(e.name);
            String receiver = e.instance() ? "INSTANCE" : target;
            String params;

            if (e.kind != null) {
                params = handler(out, e, id, receiver);
            }
            else {
                params = function(out, e, id, receiver);
            }

            if (e.role == Role.FUNCTION) {
                signature(out, e.name, id, "fn " + e.name + params + " -> value");
            }
            else if (e.kind != null) {
                String text = e.kind.constant == null
                        ? CONFLICT + ".signature(\"" + e.name + "\")"
                        : EVENTS + ".signature(\"" + e.kind.event + "\", \"" + e.name + "\", " + EVENTS + "."
                                + e.kind.constant + ")";

                signatureBytes(out, e.name, id, text);
            }
            else {
                String[] halves = streams.computeIfAbsent(e.name, k -> new String[2]);

                halves[e.role == Role.RECORD ? 0 : 1] = params;
            }
        }

        for (Map.Entry<String, String[]> s : streams.entrySet()) {
            signature(out, s.getKey(), identifier(s.getKey()), "stream " + s.getKey() + " { record"
                    + s.getValue()[0] + " end" + s.getValue()[1] + " }");
        }

        out.append("\n    /** TeaVM's entry point; a module of UDFs has nothing to run. */\n");
        out.append("    public static void main(String[] a) {\n    }\n");
        out.append("}\n");

        try {
            JavaFileObject file = processingEnv.getFiler()
                    .createSourceFile(pkg.isEmpty() ? simple : pkg + "." + simple, owner);

            try (Writer w = file.openWriter()) {
                w.write(out.toString());
            }
        }
        catch (IOException ex) {
            error(owner, "could not write " + simple + ": " + ex.getMessage());
        }
    }

    /** A record or stream UDF's export. Returns the signature's parameter text. */
    private String function(StringBuilder out, Export e, String id, String receiver) {
        String export = e.role == Role.RECORD ? e.name + "_record" : e.role == Role.END ? e.name + "_end" : e.name;
        String method = receiver + "." + e.method.getSimpleName();

        out.append("\n");

        if (e.raw()) {
            out.append("    @Export(name = \"").append(export).append("\")\n");
            out.append("    public static int ").append(prefix(e)).append(id).append("(int args, int argc) {\n");
            out.append("        try {\n");
            out.append("            return ").append(method).append("(args, argc);\n");
            catches(out, e.name);
            out.append("    }\n");

            return e.params.isEmpty() ? "()" : e.params;
        }

        List<? extends VariableElement> params = e.method.getParameters();
        List<Param> decoded = new ArrayList<>();
        boolean rest = false;

        for (int i = 0; i < params.size(); i++) {
            Param p = param(params.get(i), i, params.size(), e.name, "ARGS_" + prefix(e) + id);

            decoded.add(p);
            rest |= p.rest;
        }

        int capacity = rest ? 128 : Math.max(1, params.size());

        out.append("    private static final ").append(VALUE).append("[] ARGS_").append(prefix(e)).append(id)
                .append(" = new ").append(VALUE).append("[").append(capacity).append("];\n\n");
        out.append("    @Export(name = \"").append(export).append("\")\n");
        out.append("    public static int ").append(prefix(e)).append(id).append("(int args, int argc) {\n");
        out.append("        try {\n");
        out.append("            int n = ").append(UDF).append(".args(args, argc, ARGS_").append(prefix(e)).append(id)
                .append(");\n");

        // Udf.args fills at most the array's length, so the surplus shows only in argc.
        if (!rest) {
            out.append("            ").append(PARAMS).append(".arity(\"").append(e.name).append("\", ")
                    .append(params.size()).append(", argc);\n");
        }

        StringBuilder call = new StringBuilder(method + "(");
        StringBuilder sig = new StringBuilder("(");

        for (int i = 0; i < decoded.size(); i++) {
            call.append(i > 0 ? ", " : "").append(decoded.get(i).expr);
            sig.append(i > 0 ? ", " : "").append(decoded.get(i).sig);
        }

        call.append(")");
        sig.append(")");

        if (e.role == Role.RECORD) {
            out.append("            ").append(call).append(";\n");
        }
        else {
            out.append("            ").append(result(e.method.getReturnType(), call.toString())).append("\n");
        }

        out.append("            return 0;\n");
        catches(out, e.name);
        out.append("    }\n");

        return sig.toString();
    }

    /** An event handler's export. */
    private String handler(StringBuilder out, Export e, String id, String receiver) {
        String method = receiver + "." + e.method.getSimpleName();
        Kind k = e.kind;

        out.append("\n    @Export(name = \"").append(e.name).append("\")\n");
        out.append("    public static int ").append(prefix(e)).append(id).append("(int args, int argc) {\n");

        if (e.raw()) {
            out.append("        try {\n");
            out.append("            return ").append(method).append("(args, argc);\n");
            catches(out, e.name);
            out.append("    }\n");

            return "";
        }

        List<String> actuals = new ArrayList<>();
        String fail = "            return " + UDF + ".fail(\"" + e.name + ": not a"
                + ("aeioux".indexOf(k.event.charAt(0)) >= 0 ? "n " : " ") + k.event + " event\");\n";

        if (k.constant == null) {
            out.append("        ").append(CONFLICT).append(" conflict = new ").append(CONFLICT).append("();\n\n");
            out.append("        if (!conflict.decode(args, argc)) {\n").append(fail).append("        }\n\n");
            actuals.add("conflict.local");
            actuals.add("conflict.remote");
            actuals.add("conflict");
        }
        else if (k.event.equals("timer")) {
            out.append("        ").append(EVENTS).append(".TimerInfo timer = new ").append(EVENTS).append(".TimerInfo();\n\n");
            out.append("        if (!timer.decode(args, argc)) {\n").append(fail).append("        }\n\n");
            actuals.add("timer");
        }
        else {
            out.append("        ").append(VALUE).append(" desc = ").append(EVENTS).append(".desc(args, argc);\n");

            for (int i = 0; i < k.records.length; i++) {
                out.append("        ").append(EVENT_RECORD).append(" record").append(i).append(" = new ")
                        .append(EVENT_RECORD).append("();\n");
                actuals.add("record" + i);
            }

            String info = k.params[k.params.length - 1];

            out.append("        ").append(info).append(" info = new ").append(info).append("();\n\n");
            out.append("        if (desc == null");

            for (int i = 0; i < k.records.length; i++) {
                out.append(" || !").append(EVENTS).append(".side(desc, \"").append(k.records[i]).append("\", ")
                        .append(i).append(", record").append(i).append(")");
            }

            out.append(" || !info.decode(desc)) {\n").append(fail).append("        }\n\n");
            actuals.add("info");
        }

        String call = method + "(" + String.join(", ", actuals) + ")";

        out.append("        try {\n");

        if (k.answer == null) {
            out.append("            ").append(call).append(";\n");
            out.append("            return ").append(EVENTS).append(".done();\n");
        }
        else {
            out.append("            return ").append(call).append(".deliver();\n");
        }

        catches(out, e.name);
        out.append("    }\n");

        return "";
    }

    private static void catches(StringBuilder out, String name) {
        out.append("        }\n");
        out.append("        catch (").append(UDF_EXCEPTION).append(" e) {\n");
        out.append("            return ").append(UDF).append(".fail(e.getMessage());\n");
        out.append("        }\n");
        out.append("        catch (Throwable t) {\n");
        out.append("            return ").append(PARAMS).append(".uncaught(\"").append(name).append("\", t);\n");
        out.append("        }\n");
    }

    private static void signature(StringBuilder out, String name, String id, String text) {
        signatureBytes(out, name, id,
                UDF + ".signatureBytes(\"" + text.replace("\\", "\\\\").replace("\"", "\\\"") + "\")");
    }

    private static void signatureBytes(StringBuilder out, String name, String id, String expr) {
        out.append("\n    private static final byte[] SIG_").append(id).append(" = ").append(expr).append(";\n\n");
        out.append("    @Export(name = \"__udf_sig_").append(name).append("\")\n");
        out.append("    public static int sig_").append(id).append("() {\n");
        out.append("        return ").append(UDF).append(".signature(SIG_").append(id).append(");\n");
        out.append("    }\n");
    }

    private static String prefix(Export e) {
        switch (e.role) {
        case RECORD:
            return "record_";
        case END:
            return "end_";
        default:
            return "udf_";
        }
    }

    //==========================================================
    // Names and lookups.
    //

    private static String identifier(String name) {
        StringBuilder b = new StringBuilder();

        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);

            b.append(Character.isJavaIdentifierPart(c) ? c : '_');
        }

        return b.toString();
    }

    /** `Outer.Inner` for a nested class, the simple name otherwise. */
    private static String nestedName(TypeElement type) {
        String name = type.getSimpleName().toString();
        Element e = type.getEnclosingElement();

        while (e instanceof TypeElement) {
            name = e.getSimpleName() + "." + name;
            e = e.getEnclosingElement();
        }

        return name;
    }

    private static String qualifiedName(DeclaredType t) {
        return ((TypeElement) t.asElement()).getQualifiedName().toString();
    }

    /** The qualified name of a declared type (`com.aerospike.udf.Events.WriteInfo`), or the type's text. */
    private static String typeName(TypeMirror t) {
        return t.getKind() == TypeKind.DECLARED ? qualifiedName((DeclaredType) t) : t.toString();
    }

    private static String simple(String qualified) {
        return qualified.startsWith(SDK) ? qualified.substring(SDK.length()) : qualified;
    }

    private static AnnotationMirror annotation(Element e, String type) {
        for (AnnotationMirror m : e.getAnnotationMirrors()) {
            if (qualifiedName(m.getAnnotationType()).equals(type)) {
                return m;
            }
        }

        return null;
    }

    private static String string(AnnotationMirror mirror, String element) {
        for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> v : mirror.getElementValues().entrySet()) {
            if (v.getKey().getSimpleName().contentEquals(element)) {
                return v.getValue().getValue().toString();
            }
        }

        return "";
    }

    /** The class's method of that name, when there is exactly one. */
    private ExecutableElement method(TypeElement owner, String name) {
        ExecutableElement found = null;

        for (Element e : owner.getEnclosedElements()) {
            if (e.getKind() == ElementKind.METHOD && e.getSimpleName().contentEquals(name)) {
                if (found != null) {
                    error(e, "a @Stream class has one " + name + " method");
                    return null;
                }

                found = (ExecutableElement) e;
            }
        }

        return found;
    }

    /** A constructor without arguments the generated class can call, or null. */
    private static ExecutableElement constructor(TypeElement owner) {
        for (Element e : owner.getEnclosedElements()) {
            if (e.getKind() == ElementKind.CONSTRUCTOR && ((ExecutableElement) e).getParameters().isEmpty()
                    && !e.getModifiers().contains(Modifier.PRIVATE)) {
                return (ExecutableElement) e;
            }
        }

        return null;
    }

    private void error(Element at, String message) {
        failed = true;
        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, message, at);
    }
}
