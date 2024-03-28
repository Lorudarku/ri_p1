package es.udc.fi.ri.ri_p1;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.document.Document;
import org.jsoup.Jsoup;


import java.io.*;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WebIndexer {

    private static String docsPath;
    private static boolean titleTermVectors;
    private static boolean bodyTermVectors;
    private static Analyzer analyzer;
    private static boolean printThreadInfo ;


    public static void main(String[] args) throws IOException, InterruptedException {

        String indexPath = "index";    // Carpeta donde se almacenará el índice
        docsPath = "docs";             // Carpeta donde se almacenan los archivos .loc y .loc.notags
        boolean create = false;        // Flag para la opción create
        int numThreads = Runtime.getRuntime().availableProcessors();    // Número de hilos por defecto
        printThreadInfo = false;       // Flag para la opción threadInfo
        boolean printAppInfo = false;  // Flag para la opción appInfo
        titleTermVectors = false;      // Flag para la opción titleTermVectors
        bodyTermVectors = false;       // Flag para la opción bodyTermVectors

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docs":
                    docsPath = args[++i];
                    break;
                case "-create":
                    create = true;
                    break;
                case "-numThreads":
                    numThreads = Integer.parseInt(args[++i]);
                    break;
                case "-h":
                    printThreadInfo = true;
                    break;
                case "-p":
                    printAppInfo = true;
                    break;
                case "-titleTermVectors":
                    titleTermVectors = true;
                    break;
                case "-bodyTermVectors":
                    bodyTermVectors = true;
                    break;
                case "-analyzer":
                    String analyzerName = args[++i];
                    analyzer = getAnalyzer(analyzerName);
                    break;
                // Añadir más opciones según sea necesario
            }
        }

        // Configurar el analizador predeterminado (StandardAnalyzer si no se proporciona uno)
        if (analyzer == null) {
            analyzer = new StandardAnalyzer();
        }

        // Inicializar el directorio del índice
        Directory indexDirectory = FSDirectory.open(Path.of(indexPath));

        // Configurar el analizador
        Analyzer analyzer = new StandardAnalyzer();

        // Configurar el indexWriter
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
        if (create) {
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        } else {
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        }

        // Inicializar el IndexWriter
        IndexWriter indexWriter = new IndexWriter(indexDirectory, indexWriterConfig);

        // Configurar ThreadPool con el número de hilos especificado
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

        // Procesar archivos .url en paralelo
        Files.walk(Path.of(docsPath))
                .filter(Files::isRegularFile)
                .filter(path -> path.toString().endsWith(".url"))
                .forEach(path -> executorService.submit(() -> processUrlFile(path, indexWriter)));

        // Apagar el ThreadPool después de terminar todas las tareas
        executorService.shutdown();

        if (printAppInfo) {
            System.out.println("Creado índice " + indexPath + " en " + System.currentTimeMillis() + " msecs");
        }
    }


    private static Analyzer getAnalyzer(String analyzerName) {
        // Lógica para obtener el Analyzer correspondiente según el nombre proporcionado
        // Por ahora, solo se admite el StandardAnalyzer como predeterminado
        if ("StandardAnalyzer".equalsIgnoreCase(analyzerName)) {
            return new StandardAnalyzer();
        } else {
            // Puedes agregar lógica para admitir más Analyzers si es necesario
            System.out.println("El analizador especificado no es compatible. Se usará el StandardAnalyzer.");
            return new StandardAnalyzer();
        }
    }

    private static void processUrlFile(Path path, IndexWriter indexWriter) {
        try {
            // Leer y procesar el archivo .url
            BufferedReader reader = Files.newBufferedReader(path);
            String line;
            while ((line = reader.readLine()) != null) {
                processUrl(line, indexWriter, path);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processUrl(String url, IndexWriter indexWriter, Path urlFilePath) {
        try {
            // Mostrar información de inicio de hilo si se especifica
            if (printThreadInfo) {
                System.out.println("Hilo " + Thread.currentThread().getName() + " comienzo url " + url);
            }

            HttpClient httpClient = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10)) // Establecer timeout de conexión
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            // Verificar si la respuesta es un código de redirección 3xx
            if (response.statusCode() >= 300 && response.statusCode() < 400) {
                // Obtener la nueva URL de redirección
                String redirectUrl = response.headers().firstValue("Location").orElse(null);
                if (redirectUrl != null) {
                    // Procesar la nueva URL de redirección
                    processUrl(redirectUrl, indexWriter, urlFilePath);
                    return; // Salir del método para evitar la indexación de la URL original
                }
            }

            // Verificar si la respuesta es exitosa (código 200)
            if (response.statusCode() == 200) {
                // Parsear la página web
                org.jsoup.nodes.Document doc = Jsoup.parse(response.body());
                String title = doc.title();
                String body = doc.body().text();

                // Crear el nombre de archivo local
                String fileName = url.replaceAll("^https?://", "").replaceAll("/", "_").replaceAll("\\W+", "");
                String filePath = docsPath + "/" + fileName + ".loc.notags";

                // Escribir el contenido en el archivo .loc.notags
                try (PrintWriter writer = new PrintWriter(filePath)) {
                    writer.println(title);
                    writer.println(body);
                }

                // Obtener información del archivo .loc
                BasicFileAttributes attrs = Files.readAttributes(urlFilePath, BasicFileAttributes.class);

                // Calcular el tamaño del archivo .loc
                long locSize = Files.size(urlFilePath);

                // Calcular el tamaño del archivo .loc.notags
                long notagsSize = Files.size(Path.of(filePath));

                // Obtener información de tiempo
                Date creationTime = new Date(attrs.creationTime().toMillis());
                Date lastAccessTime = new Date(attrs.lastAccessTime().toMillis());
                Date lastModifiedTime = new Date(attrs.lastModifiedTime().toMillis());

                // Convertir las fechas al formato de Lucene
                String creationTimeLucene = DateTools.dateToString(creationTime, DateTools.Resolution.SECOND);
                String lastAccessTimeLucene = DateTools.dateToString(lastAccessTime, DateTools.Resolution.SECOND);
                String lastModifiedTimeLucene = DateTools.dateToString(lastModifiedTime, DateTools.Resolution.SECOND);

                //Opción -titleTermVector
                FieldType titleFieldType = new FieldType(TextField.TYPE_STORED);
                if (titleTermVectors) {
                    titleFieldType.setStoreTermVectors(true);
                    titleFieldType.setStoreTermVectorPositions(true);
                    titleFieldType.setStoreTermVectorOffsets(true);
                }

                //Opción -bodyTermVector
                FieldType bodyFieldType = new FieldType(TextField.TYPE_STORED);
                if (bodyTermVectors) {
                    bodyFieldType.setStoreTermVectors(true);
                    bodyFieldType.setStoreTermVectorPositions(true);
                    bodyFieldType.setStoreTermVectorOffsets(true);
                }

                // Crear documento Lucene para el archivo .loc.notags
                Document luceneDoc = new Document();
                luceneDoc.add(new StringField("url", url, Field.Store.YES));
                luceneDoc.add(new Field("title", title, titleFieldType));
                luceneDoc.add(new Field("body", body, bodyFieldType));
                luceneDoc.add(new StoredField("title", title)); // Campo adicional para ver en la pestaña de documentos de Luke
                luceneDoc.add(new StoredField("body", body)); // Campo adicional para ver en la pestaña de documentos de Luke
                luceneDoc.add(new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES));
                luceneDoc.add(new StringField("thread", Thread.currentThread().getName(), Field.Store.YES));
                luceneDoc.add(new LongPoint("locKb", locSize / 1024));
                luceneDoc.add(new StoredField("locKb", locSize / 1024)); // Campo adicional para ver en la pestaña de documentos de Luke
                luceneDoc.add(new LongPoint("notagsKb", notagsSize / 1024));
                luceneDoc.add(new StoredField("notagsKb", notagsSize / 1024)); // Campo adicional para ver en la pestaña de documentos de Luke
                luceneDoc.add(new StoredField("creationTime", creationTime.toString()));
                luceneDoc.add(new StoredField("lastAccessTime", lastAccessTime.toString()));
                luceneDoc.add(new StoredField("lastModifiedTime", lastModifiedTime.toString()));
                luceneDoc.add(new StoredField("creationTimeLucene", creationTimeLucene));
                luceneDoc.add(new StoredField("lastAccessTimeLucene", lastAccessTimeLucene));
                luceneDoc.add(new StoredField("lastModifiedTimeLucene", lastModifiedTimeLucene));

                // Indexar el documento
                indexWriter.addDocument(luceneDoc);
            }
            // Mostrar información de fin de hilo si se especifica
            if (printThreadInfo) {
                System.out.println("Hilo " + Thread.currentThread().getName() + " fin url " + url);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
