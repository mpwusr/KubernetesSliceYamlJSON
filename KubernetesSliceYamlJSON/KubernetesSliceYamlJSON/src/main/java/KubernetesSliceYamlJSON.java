import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import okhttp3.*;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.LoaderOptions;
import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Stream a multi-doc YAML to the K8s API, one create call per object.
 */
public class KubernetesSliceYamlJSON {

    private static final MediaType JSON
            = MediaType.parse("application/json");
    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private final OkHttpClient client;
    private final String apiServer;          // e.g. "https://api.cluster.example:6443"
    private final String bearerToken;
    private final String defaultNamespace;

    /* ────────────────────────── ctor ────────────────────────── */
    public KubernetesSliceYamlJSON(OkHttpClient client,
                                   String apiServer,
                                   String bearerToken,
                                   String defaultNamespace) {
        this.client = client;
        this.apiServer = apiServer.replaceAll("/+$", "");   // trim trailing /
        this.bearerToken = bearerToken.trim();
        this.defaultNamespace = defaultNamespace;
    }

    /* ────────────────────────── public API ────────────────────────── */
    public void apply(Path yamlFile) throws IOException {
        try (InputStream in = Files.newInputStream(yamlFile)) {
            // stream all YAML docs one by one
            Iterable<Object> docs = new Yaml(new SafeConstructor(new LoaderOptions())).loadAll(in);
            for (Object doc : docs) {
                if (!(doc instanceof Map<?, ?> map) || map.isEmpty()) continue;
                postSingleObject((Map<String, Object>) map);
            }
        }
    }

    /* ────────────────────────── helpers ────────────────────────── */
    private void postSingleObject(Map<String, Object> obj) throws IOException {
        String apiVersion = (String) obj.get("apiVersion");   // "v1"  OR  "coherence.oracle.com/v1"
        String kind = (String) obj.get("kind");

        if (apiVersion == null || kind == null)
            throw new IllegalArgumentException("Object missing apiVersion or kind");

        String[] gv = apiVersion.split("/", 2);
        String group = gv.length == 2 ? gv[0] : "";   // core group ⇒ ""
        String version = gv.length == 2 ? gv[1] : gv[0];

        // metadata
        Map<String, Object> meta = (Map<String, Object>) obj.getOrDefault("metadata", Map.of());
        String ns = (String) meta.getOrDefault("namespace", defaultNamespace);
        boolean namespaced = ns != null && !ns.isBlank();

        // work out the plural form
        String plural = toPlural(kind);

        // build the URL
        StringBuilder url = new StringBuilder(apiServer);
        if (group.isBlank()) {
            url.append("/api/").append(version);
        } else {
            url.append("/apis/").append(group).append('/').append(version);
        }

        if (namespaced) {
            url.append("/namespaces/")
                    .append(URLEncoder.encode(ns, StandardCharsets.UTF_8));
        }
        url.append('/').append(plural);

        byte[] bodyBytes = jsonMapper.writeValueAsBytes(obj);
        RequestBody body = RequestBody.create(bodyBytes, JSON);

        Request req = new Request.Builder()
                .url(url.toString())
                .addHeader("Authorization", "Bearer " + bearerToken)
                .addHeader("Content-Type", "application/json")
                .post(body)
                .build();

        try (Response rsp = client.newCall(req).execute()) {
            int code = rsp.code();
            String rspBody = rsp.body() != null ? rsp.body().string() : "<empty>";
            System.out.printf("%s %s → %d%n%s%n%n",
                    kind, meta.get("name"), code, rspBody);
        }
    }

    /* crude pluralisation with a few special-cases */
    private static final Map<String, String> SPECIAL_PLURALS = Map.of(
            "ConfigMap", "configmaps",
            "Secret", "secrets",
            "Ingress", "ingresses",
            "Service", "services",
            "Coherence", "coherence"     // the Oracle Coherence CRD!
    );

    private String toPlural(String kind) {
        return SPECIAL_PLURALS.getOrDefault(
                kind,
                kind.toLowerCase(Locale.ROOT) + "s");
    }

    /* ────────────────────────── quick demo ────────────────────────── */
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: KubernetesSliceYamlJSON <api-server> <token-file> <namespace> <yaml-file>");
            System.exit(1);
        }

        // 1. Load token
        String token = Files.readString(Path.of(args[1]));

        // 2. Load CA cert from ca.crt and set up trust
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

        // ✅ Define OkHttpClient INSIDE main
        OkHttpClient client = new OkHttpClient.Builder()
                .sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) tmf.getTrustManagers()[0])
                .build();

        // 3. Run the YAML loader
        new KubernetesSliceYamlJSON(client, args[0], token, args[2])
                .apply(Path.of(args[3]));
    }
}


