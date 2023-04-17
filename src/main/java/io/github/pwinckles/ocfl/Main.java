package io.github.pwinckles.ocfl;

import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import edu.wisc.library.ocfl.aws.OcflS3Client;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.crt.Log;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final long FILE_BYTES = 100 * 1024 * 1024;
    private static final String FILE_NAME = "file.txt";

    public static void main(String[] args) {
        var config = readConfig();

        try {
            log.info("Creating client using config: {}", config);

            var work = Files.createDirectories(Paths.get("ocfl-s3-test"));
            var repo = createRepo(config, work);
            var uuid = UUID.randomUUID();
            var id = "urn:uuid:" + uuid;

            var file = generateFile(work);
            writeObject(file, id, repo);

            var output = work.resolve(uuid.toString());

            getObject(output, id, repo);
            validateObject(output);

            log.info("Test passed");
        } catch (Exception e) {
            log.error("Test failed", e);
            System.exit(1);
        }
    }

    private static Path generateFile(Path work) throws IOException {
        var file = work.resolve(FILE_NAME);
        if (Files.notExists(file)) {
            log.info("Generating test file...");
            writeFile(file, FILE_BYTES);
        }
        return file;
    }

    private static void writeObject(Path file, String id, OcflRepository repo) {
        log.info("Writing 100MB object {}...", id);

        var start = Instant.now();

        try {
            repo.updateObject(ObjectVersionId.head(id),
                    new VersionInfo()
                            .setUser("test", "test@example.com")
                            .setMessage("s3 transfer manager test"), updater -> {
                        updater.addPath(file, FILE_NAME);
                    });
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to write object", e);
        }

        log.info("Object successfully written to S3 in {}", Duration.between(start, Instant.now()));
    }

    private static void getObject(Path output, String id, OcflRepository repo) {
        log.info("Downloading object to {}...", output);

        var start = Instant.now();

        try {
            repo.getObject(ObjectVersionId.head(id), output);
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to get object", e);
        }

        log.info("Object successfully download from S3 in {}", Duration.between(start, Instant.now()));
    }

    private static void validateObject(Path output) throws IOException {
        var file = output.resolve(FILE_NAME);

        if (Files.notExists(file)) {
            throw new RuntimeException(String.format("Expected %s to exist, but it does not", file));
        }

        if (Files.size(file) != FILE_BYTES) {
            throw new RuntimeException(String.format("Expected %s to be size %s, but was %s", FILE_BYTES, Files.size(file), file));
        }
    }

    private static Config readConfig() {
        var reader = new Scanner(System.in);

        System.out.print("Profile [default]: ");
        var profile = defaulted(reader.nextLine(), "default");

        System.out.print("Region [us-east-2]: ");
        var region = defaulted(reader.nextLine(), "us-east-2");

        System.out.print("Endpoint: ");
        var endpoint = defaulted(reader.nextLine(), null);

        String bucket = null;
        while (bucket == null) {
            System.out.print("Bucket: ");
            bucket = defaulted(reader.nextLine(), null);
        }

        System.out.print("Prefix: ");
        var prefix = defaulted(reader.nextLine(), null);

        return new Config(profile, region, endpoint, bucket, prefix);
    }

    private static OcflRepository createRepo(Config config, Path work) throws IOException {
        var ocflWork = Files.createDirectories(work.resolve("ocfl-temp"));

        Log.initLoggingToFile(Log.LogLevel.Trace, "aws-sdk.log");
        var clientBuilder = S3AsyncClient.crtBuilder()
                .credentialsProvider(ProfileCredentialsProvider.builder()
                        .profileName(config.profile)
                        .build())
                .region(Region.of(config.region));

        if (config.endpoint != null) {
            clientBuilder.endpointOverride(URI.create(config.endpoint));
        }

        return new OcflRepositoryBuilder()
                .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                .storage(storage -> storage.cloud(OcflS3Client.builder()
                        .s3Client(clientBuilder.build())
                        .bucket(config.bucket)
                        .repoPrefix(config.prefix)
                        .build()))
                .workDir(ocflWork)
                .build();
    }

    private static void writeFile(Path path, long size) throws IOException {
        var bytes = new byte[8192];
        try (var out = new BufferedOutputStream(Files.newOutputStream(path))) {
            var written = 0;
            while (written < size) {
                ThreadLocalRandom.current().nextBytes(bytes);
                int len = (int) Math.min(8192, size - written);
                out.write(bytes, 0, len);
                written += len;
            }
        }
    }

    private static String defaulted(String value, String defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value;
    }

    private record Config(String profile, String region, String endpoint, String bucket, String prefix) {}

}
