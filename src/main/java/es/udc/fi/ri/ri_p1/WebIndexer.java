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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WebIndexer {

    private static String docsPath;
    private static String urlPath;
    private static boolean titleTermVectors;
    private static boolean bodyTermVectors;
    private static Analyzer analyzer;
    private static boolean printThreadInfo ;
    private static String[] onlyDoms;

    public static class WorkerThread implements Runnable {
        private final Path folder;
        private final IndexWriter writer;
        public WorkerThread(final Path folder, IndexWriter writer) {
            this.folder = folder;
            this.writer = writer;
        }
        /**
         * This is the work that the current thread will do when processed by the pool.
         * In this case, it will only print some information.
         */
        @Override
        public void run() {
            System.out.println(String.format("I am the thread '%s' and I am responsible for folder '%s'",
                    Thread.currentThread().getName(), folder));

            processUrlFile(folder, writer);
        }

    }
    public static void main(String[] args) throws IOException, InterruptedException {

        String indexPath = "src/main/resources/index";    // Carpeta donde se almacenará el índice
        docsPath = "src/main/resources/docs";             // Carpeta donde se almacenan los archivos .loc y .loc.notags
        urlPath = "src/test/resources/urls";              // Carpeta donde se almacenan los archivos .url
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

        //Cargamos el fichero de propiedades
        Properties properties = new Properties();
        properties.load(new FileInputStream("src/main/resources/config.properties"));

        //Obtenemos los valores de las propiedades
        String propOnlyDoms = properties.getProperty("onlyDoms");
        if (propOnlyDoms != null) {
            onlyDoms = propOnlyDoms.split("\\s+");
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

        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(urlPath))) {

            /* We process each subfolder in a new thread. */
            for (final Path path : directoryStream) {
                if (Files.isRegularFile(path)) {
                    final Runnable worker = new WorkerThread(path, indexWriter);
                    /*
                     * Send the thread to the ThreadPool. It will be processed eventually.
                     */
                    executorService.execute(worker);
                }
            }

        } catch (final IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        // Apagar el ThreadPool después de terminar todas las tareas
        executorService.shutdown();

        /* Wait up to 1 hour to finish all the previously submitted jobs */
        try {
            executorService.awaitTermination(1, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }

        // Cerrar el IndexWriter
        indexWriter.close();

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
                // Filtrar las URL según el archivo de propiedades
                if (onlyDoms != null) {
                    boolean found = false;
                    for (String dom : onlyDoms) {
                        if (line.contains(dom)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        continue;
                    }
                }
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
                } else if (response.statusCode() == 200) { // Si la respuesta es 200 OK
                    String fileName = url.replaceAll("^https?://", "").replaceAll("/", "_").replaceAll("\\W+", "");
                    String locfilePath = docsPath + "/" + fileName + ".loc.";
                    String locnotagsfilePath = docsPath + "/" + fileName + ".loc.notags";


                    Path rutaLoc = Paths.get(locfilePath);
                    Files.write(rutaLoc, response.body().getBytes());

                    // Parsear la página web
                    org.jsoup.nodes.Document doc = Jsoup.parse(response.body());
                    String title = doc.title();
                    String body = doc.body().text();

                    // Crear el nombre de archivo local

                    // Escribir el contenido en el archivo .loc.notags
                    try (PrintWriter writer = new PrintWriter(locnotagsfilePath)) {
                        writer.println(title);
                        writer.println(body);
                    }

                    // Obtener información del archivo .loc
                    BasicFileAttributes attrs = Files.readAttributes(urlFilePath, BasicFileAttributes.class);

                    // Calcular el tamaño del archivo .loc
                    long locSize = Files.size(rutaLoc);

                    // Calcular el tamaño del archivo .loc.notags
                    long notagsSize = Files.size(Path.of(locnotagsfilePath));

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
                    luceneDoc.add(new StringField("path", locnotagsfilePath, Field.Store.YES));
                    luceneDoc.add(new Field("title", title, titleFieldType));
                    luceneDoc.add(new Field("body", body, bodyFieldType));
                    //luceneDoc.add(new StoredField("title", title)); // Campo adicional para ver en la pestaña de documentos de Luke
                    //luceneDoc.add(new StoredField("body", body)); // Campo adicional para ver en la pestaña de documentos de Luke
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

                    if (indexWriter.getConfig().getOpenMode() == IndexWriterConfig.OpenMode.CREATE) {
                        // New index, so we just add the document (no old document can be there):
                        System.out.println("adding " + urlFilePath);
                        indexWriter.addDocument(luceneDoc);
                    } else {
                        // Existing index (an old copy of this document may have been indexed) so
                        // we use updateDocument instead to replace the old one matching the exact
                        // path, if present:
                        System.out.println("updating " + urlFilePath);
                        indexWriter.updateDocument(new Term("path", urlFilePath.toString()), luceneDoc);
                    }
                } else { //Si no se puede acceder a la URL
                    System.out.println("Error al procesar la URL: " + url + " - Código de estado: " + response.statusCode());
                }

                // Mostrar información de fin de hilo si se especifica
                if (printThreadInfo) {
                    System.out.println("Hilo " + Thread.currentThread().getName() + " fin url " + url);
                }
        } catch (IOException | InterruptedException e) { // Manejar excepciones de E/S y de red
            e.printStackTrace();
        }
    }
}
