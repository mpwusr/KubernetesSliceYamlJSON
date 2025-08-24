import okhttp3.*;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class KubernetesSliceYamlJSON {

    private static final OkHttpClient client = new OkHttpClient();
    private static final ObjectMapper mapper = new ObjectMapper();
    private static byte[] caCert;
    private static String apiServer;
    private static String token;
    private static String targetNamespace;

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: java -jar KubernetesSliceYamlJSON.jar <apiserver> <tokenFile> <namespace> <instructions.json>");
            System.exit(1);
        }

        apiServer = args[0];
        token = new String(Files.readAllBytes(Paths.get(args[1]))).trim();
        targetNamespace = args[2];

        File instructions = new File(args[3]);
        List<Map<String, Object>> resources = loadInstructions(instructions);

        for (Map<String, Object> resource : resources) {
            processInstructions(resource);
        }
    }

    private static List<Map<String, Object>> loadInstructions(File instructions) throws IOException {
        String ext = getExtension(instructions.getName());
        if (ext.equalsIgnoreCase("yaml") || ext.equalsIgnoreCase("yml")) {
            return new ArrayList<>(new Yaml(new SafeConstructor()).loadAll(new FileReader(instructions)));
        } else if (ext.equalsIgnoreCase("json")) {
            return mapper.readValue(instructions, List.class);
        }
        throw new IllegalArgumentException("Unsupported file extension: " + ext);
    }

    private static void processInstructions(Map<String, Object> obj) throws Exception {
        String kind = (String) obj.get("kind");
        String metadataName = ((Map<String, Object>) obj.get("metadata")).get("name").toString();

        // Override namespace
        String nsToUse = targetNamespace;
        if (obj.containsKey("metadata")) {
            Map<String, Object> metadata = (Map<String, Object>) obj.get("metadata");
            metadata.put("namespace", nsToUse);
        }

        // Get action (create/replace)
        String action = (String) obj.getOrDefault("action", "create");

        switch (action.toLowerCase()) {
            case "create" -> {
                createWith409Replace(obj, kind, nsToUse);
            }
            case "replace" -> {
                if ("Pod".equalsIgnoreCase(kind)) {
                    // Pods cannot be replaced: delete + create
                    System.out.printf("%s/%s replace → delete+create%n", kind, metadataName);
                    deleteResource(kind, nsToUse, metadataName, group(obj), version(obj));
                    // Sleep a bit to avoid race conditions
                    Thread.sleep(1000);
                    createWith409Replace(obj, kind, nsToUse);
                    continue; // *** CRITICAL: skip replaceResource() ***
                } else {
                    replaceResource(obj, kind, nsToUse, metadataName);
                }
            }
            default -> {
                System.out.printf("Unknown action %s for %s/%s%n", action, kind, metadataName);
            }
        }
    }

    private static void createWith409Replace(Map<String, Object> obj, String kind, String namespace) throws Exception {
        String name = ((Map<String, Object>) obj.get("metadata")).get("name").toString();

        String url = buildUrl(kind, namespace, name, false);
        String jsonBody = mapper.writeValueAsString(obj);

        Request request = new Request.Builder()
                .url(url)
                .post(RequestBody.create(jsonBody, MediaType.parse("application/json")))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (response.code() == 409) {
                // Conflict → replace existing resource
                replaceResource(obj, kind, namespace, name);
            } else if (!response.isSuccessful()) {
                System.out.printf("%s/%s create → %d%n", kind, name, response.code());
                System.out.println(response.body().string());
            } else {
                System.out.printf("%s/%s created → %d%n", kind, name, response.code());
                System.out.println(response.body().string());
            }
        }
    }

    private static void replaceResource(Map<String, Object> obj, String kind, String namespace, String name) throws Exception {
        String url = buildUrl(kind, namespace, name, true);
        String jsonBody = mapper.writeValueAsString(obj);

        Request request = new Request.Builder()
                .url(url)
                .put(RequestBody.create(jsonBody, MediaType.parse("application/json")))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                System.out.printf("%s/%s replace → %d%n", kind, name, response.code());
                System.out.println(response.body().string());
            } else {
                System.out.printf("%s/%s replace → %d%n", kind, name, response.code());
                System.out.println(response.body().string());
            }
        }
    }

    private static void deleteResource(String kind, String namespace, String name, String group, String version) throws Exception {
        String url = buildUrl(kind, namespace, name, true);

        Request request = new Request.Builder()
                .url(url)
                .delete()
                .header("Authorization", "Bearer " + token)
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful() && response.code() != 404) {
                System.out.printf("%s/%s delete → %d%n", kind, name, response.code());
                System.out.println(response.body().string());
            }
        }
    }

    private static String buildUrl(String kind, String namespace, String name, boolean includeName) {
        String resourcePath = pluralizeKind(kind);
        String base = kind.equalsIgnoreCase("Namespace") ?
                "/api/v1/namespaces" :
                "/api/v1/namespaces/" + namespace + "/" + resourcePath;

        return apiServer + base + (includeName ? "/" + name : "");
    }

    private static String pluralizeKind(String kind) {
        switch (kind.toLowerCase()) {
            case "service": return "services";
            case "pod": return "pods";
            case "configmap": return "configmaps";
            case "statefulset": return "statefulsets";
            case "deployment": return "deployments";
            case "namespace": return "namespaces";
            default: return kind.toLowerCase() + "s";
        }
    }

    private static String group(Map<String, Object> obj) {
        return (String) obj.getOrDefault("apiGroup", "");
    }

    private static String version(Map<String, Object> obj) {
        return (String) obj.getOrDefault("apiVersion", "v1");
    }

    private static String getExtension(String filename) {
        int idx = filename.lastIndexOf('.');
        return idx > 0 ? filename.substring(idx + 1) : "";
    }
}
