package es.udc.fi.ri.ri_p1;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

public class TopTermsInField {

    public static void main(String[] args) {
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
        try {
            Directory dir = FSDirectory.open(Paths.get(indexPath));
            IndexReader reader = DirectoryReader.open(dir);
            Terms terms = MultiTerms.getTerms(reader, field);

            if (terms != null) {
                Map<String, Long> termFreqMap = new HashMap<>();

                TermsEnum termsEnum = terms.iterator();
                BytesRef term;
                while ((term = termsEnum.next()) != null) {
                    String termText = term.utf8ToString();
                    long docFreq = termsEnum.docFreq();
                    termFreqMap.put(termText, docFreq);
                }

                // Ordenar los términos por frecuencia de documento (df)
                List<Map.Entry<String, Long>> sortedTerms = new ArrayList<>(termFreqMap.entrySet());
                sortedTerms.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));

                // Obtener los primeros N términos
                List<Map.Entry<String, Long>> topTerms = sortedTerms.subList(0, Math.min(topN, sortedTerms.size()));

                // Mostrar los términos por pantalla y escribirlos en el archivo de salida
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(outfilePath))) {
                    for (Map.Entry<String, Long> entry : topTerms) {
                        String termText = entry.getKey();
                        long docFreq = entry.getValue();
                        System.out.println("Término: " + termText + ", Frecuencia de documento: " + docFreq);
                        writer.write("Término: " + termText + ", Frecuencia de documento: " + docFreq + "\n");
                    }
                }
            } else {
                System.out.println("El campo especificado no existe en el índice.");
            }

            reader.close();
        } catch (IOException e) {
            System.err.println("Error al abrir el índice: " + e.getMessage());
        }
    }
}
