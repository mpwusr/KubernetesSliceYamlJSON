import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import okhttp3.*;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.LoaderOptions;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * KubernetesSliceYamlJSON v4
 *
 * - Processes Namespace resources (creates/replaces them too)
 * - For namespaced resources, overrides metadata.namespace with the CLI defaultNamespace
 * - Pods: replace = delete + create (because Pod replace is not allowed)
 * - Instructions.json format:
 *   [
 *     { "uri": "file:///path/to/file.yaml", "action": "create" },
 *     { "uri": "file:///path/to/file.yaml", "action": "replace" }
 *   ]
 */
public class KubernetesSliceYamlJSON {

    private static final MediaType JSON_MEDIA = MediaType.parse("application/json");
    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    private static final ObjectMapper jsonMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final OkHttpClient client;
    private final String apiServer;
    private final String bearerToken;
    private final String defaultNamespace;

    public KubernetesSliceYamlJSON(OkHttpClient client,
                                   String apiServer,
                                   String bearerToken,
                                   String defaultNamespace) {
        this.client = client;
        this.apiServer = apiServer.replaceAll("/+$", "");
        this.bearerToken = bearerToken.trim();
        this.defaultNamespace = defaultNamespace;
    }

    /* =======================================================================
       Instruction JSON: { uri, action }
       ======================================================================= */

    public static class Instruction {
        public String uri;
        public String action;
    }

    public void processInstructions(Path jsonFile) throws IOException {
        List<Instruction> items = jsonMapper.readValue(
                Files.readString(jsonFile), new TypeReference<List<Instruction>>() {});

        for (Instruction ins : items) {
            String action = (ins.action == null ? "create" : ins.action.toLowerCase(Locale.ROOT)).trim();

            try (InputStream in = openUriStream(ins.uri)) {
                List<Map<String, Object>> docs = readAllYamlDocs(in);
                for (Map<String, Object> obj : docs) {
                    if (obj == null || obj.isEmpty()) continue;

                    String kind = (String) obj.get("kind");
                    if (kind == null || kind.isBlank()) {
                        System.out.println("Skipping document without kind.");
                        continue;
                    }

                    Map<String, Object> meta = new LinkedHashMap<>((Map<String, Object>) obj.getOrDefault("metadata", Map.of()));
                    String name = (String) meta.get("name");
                    if (name == null || name.isBlank()) {
                        throw new IllegalArgumentException("metadata.name missing for kind=" + kind);
                    }

                    // Namespace override logic:
                    if (!"Namespace".equalsIgnoreCase(kind)) {
                        // For namespaced resources, force defaultNamespace
                        meta.put("namespace", defaultNamespace);
                        obj.put("metadata", meta);
                    }

                    String nsToUse = "Namespace".equalsIgnoreCase(kind) ? null : defaultNamespace;

                    switch (action) {
                        case "create" -> createWith409Replace(obj, kind, nsToUse);
                        case "replace" -> {
                            if ("Pod".equalsIgnoreCase(kind)) {
                                // delete + create for Pod replace
                                deleteResource(kind, nsToUse, name, group(obj), version(obj));
                                createWith409Replace(obj, kind, nsToUse);
                            } else {
                                replaceResource(obj, kind, nsToUse, name);
                            }
                        }
                        case "delete" -> {
                            deleteResource(kind, nsToUse, name, group(obj), version(obj));
                        }
                        default -> throw new IllegalArgumentException("Unknown action: " + action);
                    }
                }
            }
        }
    }

    /* =======================================================================
       Resource methods
       ======================================================================= */

    private void createWith409Replace(Map<String, Object> obj, String kind, String ns) throws IOException {
        String name = (String) meta(obj).get("name");
        ensureKindAndName(kind, name);

        String url = collectionUrl(kind, group(obj), version(obj), ns);
        RequestBody body = RequestBody.create(jsonMapper.writeValueAsBytes(obj), JSON_MEDIA);

        Request req = baseReq(url).post(body).build();
        try (Response rsp = client.newCall(req).execute()) {
            int code = rsp.code();
            String bodyStr = rsp.body() != null ? rsp.body().string() : "<empty>";
            if (code == 201) {
                log(kind, name, code, "created", bodyStr);
            } else if (code == 409) {
                replaceResource(obj, kind, ns, name);
            } else {
                log(kind, name, code, "create", bodyStr);
            }
        }
    }

    private void replaceResource(Map<String, Object> obj, String kind, String ns, String name) throws IOException {
        ensureKindAndName(kind, name);
        String url = itemUrl(kind, group(obj), version(obj), ns, name);
        RequestBody body = RequestBody.create(jsonMapper.writeValueAsBytes(obj), JSON_MEDIA);

        Request req = baseReq(url).put(body).build();
        try (Response rsp = client.newCall(req).execute()) {
            int code = rsp.code();
            String bodyStr = rsp.body() != null ? rsp.body().string() : "<empty>";
            log(kind, name, code, "replace", bodyStr);
        }
    }

    private void deleteResource(String kind, String ns, String name, String group, String version) throws IOException {
        ensureKindAndName(kind, name);
        String url = itemUrl(kind, group, version, ns, name);
        Request req = baseReq(url).delete().build();

        try (Response rsp = client.newCall(req).execute()) {
            int code = rsp.code();
            String bodyStr = rsp.body() != null ? rsp.body().string() : "<empty>";
            log(kind, name, code, "delete", bodyStr);
        }
    }

    /* =======================================================================
       Helpers
       ======================================================================= */

    private Request.Builder baseReq(String url) {
        return new Request.Builder()
                .url(url)
                .addHeader("Authorization", "Bearer " + bearerToken)
                .addHeader("Content-Type", "application/json");
    }

    private static Map<String, Object> meta(Map<String, Object> obj) {
        return (Map<String, Object>) obj.getOrDefault("metadata", Map.of());
    }

    private static String group(Map<String, Object> obj) {
        String apiVersion = (String) obj.get("apiVersion");
        if (apiVersion == null) throw new IllegalArgumentException("apiVersion missing");
        String[] gv = apiVersion.split("/", 2);
        return gv.length == 2 ? gv[0] : "";
    }

    private static String version(Map<String, Object> obj) {
        String apiVersion = (String) obj.get("apiVersion");
        if (apiVersion == null) throw new IllegalArgumentException("apiVersion missing");
        String[] gv = apiVersion.split("/", 2);
        return gv.length == 2 ? gv[1] : gv[0];
    }

    private void ensureKindAndName(String kind, String name) {
        if (kind == null || kind.isBlank()) throw new IllegalArgumentException("kind missing");
        if (name == null || name.isBlank()) throw new IllegalArgumentException("metadata.name missing");
    }

    private String collectionUrl(String kind, String group, String version, String ns) {
        StringBuilder url = new StringBuilder(apiServer);
        if (group == null || group.isBlank()) {
            url.append("/api/").append(version);
        } else {
            url.append("/apis/").append(group).append('/').append(version);
        }
        if (ns != null && !ns.isBlank()) {
            url.append("/namespaces/").append(URLEncoder.encode(ns, StandardCharsets.UTF_8));
        }
        url.append('/').append(toPlural(kind));
        return url.toString();
    }

    private String itemUrl(String kind, String group, String version, String ns, String name) {
        return collectionUrl(kind, group, version, ns) + "/" +
                URLEncoder.encode(name, StandardCharsets.UTF_8);
    }

    private static List<Map<String, Object>> readAllYamlDocs(InputStream in) {
        Iterable<Object> docs = new Yaml(new SafeConstructor(new LoaderOptions())).loadAll(in);
        List<Map<String, Object>> out = new ArrayList<>();
        for (Object o : docs) {
            if (o instanceof Map<?, ?> m && !m.isEmpty()) {
                out.add((Map<String, Object>) m);
            }
        }
        return out;
    }

    private static InputStream openUriStream(String uri) throws IOException {
        if (uri == null) throw new IllegalArgumentException("uri is null");
        if (uri.startsWith("file://")) {
            return Files.newInputStream(Path.of(URI.create(uri)));
        } else if (uri.startsWith("http://") || uri.startsWith("https://")) {
            return new URL(uri).openStream();
        } else {
            Path p = Paths.get(uri);
            if (Files.exists(p)) return Files.newInputStream(p);
            throw new IllegalArgumentException("Unsupported URI: " + uri);
        }
    }

    private static final Map<String, String> SPECIAL_PLURALS = Map.ofEntries(
            Map.entry("ConfigMap", "configmaps"),
            Map.entry("Secret", "secrets"),
            Map.entry("Ingress", "ingresses"),
            Map.entry("Service", "services"),
            Map.entry("Endpoints", "endpoints"),
            Map.entry("Pod", "pods"),
            Map.entry("StatefulSet", "statefulsets"),
            Map.entry("Deployment", "deployments"),
            Map.entry("DaemonSet", "daemonsets"),
            Map.entry("Job", "jobs"),
            Map.entry("CronJob", "cronjobs"),
            Map.entry("Namespace", "namespaces")
    );

    private String toPlural(String kind) {
        return SPECIAL_PLURALS.getOrDefault(kind, kind.toLowerCase(Locale.ROOT) + "s");
    }

    private static void log(String kind, String name, int code, String op, String body) {
        System.out.printf("%s/%s %s → %d%n%s%n%n", kind, name, op, code, body);
    }

    /* =======================================================================
       Main
       ======================================================================= */
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: KubernetesSliceYamlJSON <api-server> <token-file> <default-namespace> <instructions.json>");
            System.exit(1);
        }

        String apiServer = args[0];
        String token = Files.readString(Path.of(args[1]));
        String defaultNamespace = args[2];
        Path instructionsPath = Path.of(args[3]);

        // TLS setup from ca.crt
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Certificate ca;
        try (InputStream caInput = Files.newInputStream(Path.of("ca.crt"))) {
            ca = cf.generateCertificate(caInput);
        }

        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        ks.setCertificateEntry("ca", ca);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, tmf.getTrustManagers(), new SecureRandom());

        OkHttpClient client = new OkHttpClient.Builder()
                .sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) tmf.getTrustManagers()[0])
                .build();

        KubernetesSliceYamlJSON k = new KubernetesSliceYamlJSON(client, apiServer, token, defaultNamespace);
        k.processInstructions(instructionsPath);
    }
}
