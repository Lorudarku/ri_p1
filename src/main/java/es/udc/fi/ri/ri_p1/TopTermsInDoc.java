package es.udc.fi.ri.ri_p1;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

public class TopTermsInDoc {
    public static void main(String[] args) throws IOException {
        // Parsear argumentos de línea de comandos
        String indexPath = null;
        String field = null;
        int docID = -1;
        int topN = -1;
        String outFile = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-field":
                    field = args[++i];
                    break;
                case "-docID":
                    docID = Integer.parseInt(args[++i]);
                    break;
                case "-top":
                    topN = Integer.parseInt(args[++i]);
                    break;
                case "-outfile":
                    outFile = args[++i];
                    break;
                default:
                    System.err.println("Uso: java TopTermsInDoc -index <index_path> -field <field_name> -docID <docID> -top <topN> -outfile <output_file>");
                    System.exit(1);
            }
        }

        if (indexPath == null || field == null || docID == -1 || topN == -1 || outFile == null) {
            System.err.println("Todos los argumentos son requeridos.");
            System.exit(1);
        }

        // Abrir el índice
        Directory dir = FSDirectory.open(Paths.get(indexPath));

        try (IndexReader reader = DirectoryReader.open(dir)) {
            // Obtener el término vector del documento especificado
            Terms terms = reader.getTermVector(docID, field);

            if (terms == null) {
                System.err.println("No se encontró el documento con el ID especificado.");
                System.exit(1);
            }

            // Calcular tf-idf y ordenar términos
            List<TermWithStats> termList = calculateTermStats(terms, reader, field, docID);
            termList.sort(Comparator.comparingDouble(TermWithStats::getTfIdf).reversed());

            // Presentar los términos top n
            presentTopTerms(termList, topN, outFile);
        }
        // Cerrar el lector del índice
    }

    private static List<TermWithStats> calculateTermStats(Terms terms, IndexReader reader, String field, int docID) throws IOException {
        List<TermWithStats> termList = new ArrayList<>();
        TermsEnum iterator = terms.iterator();

        while (iterator.next() != null) {
            String termText = iterator.term().utf8ToString();
            long termFreq = iterator.totalTermFreq();
            long docFreq = reader.docFreq(new Term(field, termText));
            double idf = Math.log10((double) reader.numDocs() / (docFreq + 1)); // +1 para evitar la división por cero
            double tfIdf = termFreq * idf;

            termList.add(new TermWithStats(termText, termFreq, docFreq, tfIdf));
        }

        return termList;
    }

    private static void presentTopTerms(List<TermWithStats> termList, int topN, String outFile) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(outFile))) {
            for (int i = 0; i < Math.min(topN, termList.size()); i++) {
                TermWithStats term = termList.get(i);
                String output = String.format("Term: %s, TF: %d, DF: %d, TF-IDF: %.2f", term.getTermText(), term.getTermFreq(), term.getDocFreq(), term.getTfIdf());
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