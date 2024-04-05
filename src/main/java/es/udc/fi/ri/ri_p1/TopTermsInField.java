package es.udc.fi.ri.ri_p1;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class TopTermsInField {

    public static void main(String[] args) throws IOException {
        // Argumentos esperados: -index <ruta del índice> -field <nombre del campo> -top <cantidad de términos a mostrar> -outfile <ruta del archivo de salida>
        if (args.length < 8) {
            System.out.println("Uso: java TopTermsInField -index <ruta del índice> -field <nombre del campo> -top <cantidad de términos a mostrar> -outfile <ruta del archivo de salida>");
            System.exit(1);
        }

        // Variables para almacenar los argumentos
        String indexPath = null;
        String field = null;
        int topN = 0;
        String outfilePath = null;

        // Analizar los argumentos de la línea de comandos
        for (int i = 0; i < args.length; i++) {
            switch (args[i]){
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-field":
                    field = args[++i];
                    break;
                case "-top":
                    topN = Integer.parseInt(args[++i]);
                    break;
                case "-outfile":
                    outfilePath = args[++i];
                    break;
                default:
                    System.err.println("Uso: java TopTermsInField -index <index_path> -field <field_name> -top <topN> -outfile <output_file>");
                    System.exit(1);
            }

        }

        // Verificar que todos los argumentos hayan sido proporcionados
        if (indexPath == null || field == null || topN == 0 || outfilePath == null) {
            System.out.println("Faltan argumentos.");
            System.exit(1);
        }

        // Procesar el índice y obtener los términos del campo especificado
        Directory dir = FSDirectory.open(Paths.get(indexPath));
        IndexReader reader = DirectoryReader.open(dir);

        // Calcular tf-idf y ordenar términos
        List<TopTermsInDoc.TermWithStats> termList = calculateTermStats(reader, field);
        termList.sort(Comparator.comparingDouble(TopTermsInDoc.TermWithStats::getDocFreq).reversed());

        // Presentar los términos top n
        presentTopTerms(termList, topN, outfilePath);


    }
    private static List<TopTermsInDoc.TermWithStats> calculateTermStats(IndexReader reader, String field) throws IOException {
        List<TopTermsInDoc.TermWithStats> termList = new ArrayList<>();
        Terms terms = MultiTerms.getTerms(reader, field);
        TermsEnum iterator = terms.iterator();

        while (iterator.next() != null) {
            String termText = iterator.term().utf8ToString();

            long termFreq = iterator.totalTermFreq();
            long docFreq = reader.docFreq(new Term(field, termText));
            double idf = Math.log10((double) reader.numDocs() / (docFreq + 1)); // +1 para evitar la división por cero
            double tfIdf = termFreq * idf;

            termList.add(new TopTermsInDoc.TermWithStats(termText, termFreq, docFreq, tfIdf));
        }
        return termList;
    }
    private static void presentTopTerms(List<TopTermsInDoc.TermWithStats> termList, int topN, String outFile) throws IOException {
        // Verificar si el archivo de salida existe
        if (!Files.exists(Paths.get(outFile))) {
            // Si el archivo no existe, crearlo
            Files.createFile(Paths.get(outFile));
        }

        try (PrintWriter writer = new PrintWriter(new FileWriter(outFile))) {
            for (int i = 0; i < Math.min(topN, termList.size()); i++) {
                TopTermsInDoc.TermWithStats term = termList.get(i);
                String output = String.format("Term=%s | Doc Frequency=%d | Term Frequency=%d | TF-IDF=%.2f", term.getTermText(), term.getDocFreq(), term.getTermFreq(), term.getTfIdf());
                System.out.println(output);
                writer.println(output);
            }
        }
    }
    static class TermWithStats {
        private final String termText;
        private final long termFreq;
        private final long docFreq;
        private final double tfIdf;

        public TermWithStats(String termText, long termFreq, long docFreq, double tfIdf) {
            this.termText = termText;
            this.termFreq = termFreq;
            this.docFreq = docFreq;
            this.tfIdf = tfIdf;
        }

        public String getTermText() {
            return termText;
        }

        public long getTermFreq() {
            return termFreq;
        }

        public long getDocFreq() {
            return docFreq;
        }

        public double getTfIdf() {
            return tfIdf;
        }
    }
}
