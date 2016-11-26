package de.hbz.introx.mf.benchmarks.stream.converter;

import de.hbz.introx.mf.stream.converter.JsonToElasticsearchBulkIdKeyOriginal;
import de.hbz.introx.mf.stream.converter.JsonToElasticsearchBulkIdKeyPointer;
import de.hbz.introx.mf.stream.converter.JsonToElasticsearchBulkIdPathFind;
import de.hbz.introx.mf.stream.converter.JsonToElasticsearchBulkIdPathGet;
import org.culturegraph.mf.stream.converter.JsonEncoder;
import org.culturegraph.mf.stream.converter.xml.MarcXmlHandler;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.sink.ObjectWriter;
import org.culturegraph.mf.stream.source.FileOpener;

import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;

@Fork(2)
@Warmup(iterations = 8)
@Measurement(iterations = 8)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class JsonToElasticsearchBulkIdBenchmark {

    @Param({
        "baseline",
        "idKeyOriginal:001",
        "idKeyPointer:/001", "idKeyPointer:/040  /a",
        "idPathGet:001", "idPathGet:040  .a",
        "idPathFind:001", "idPathFind:040  .a"
    })
    private String param;

    @Benchmark
    public void benchmark() {
        final FileOpener opener = new FileOpener();
        final JsonEncoder encoder = new JsonEncoder();

        opener
            .setReceiver(new XmlDecoder())
            .setReceiver(new MarcXmlHandler())
            .setReceiver(encoder);

        configure(encoder);

        opener.process(fileName("xml"));
        opener.closeStream();
    }

    private void configure(final JsonEncoder encoder) {
        final String[] params = param.split(":", 2);

        switch (params[0]) {
            case "baseline":
                encoder
                    .setReceiver(writer());
                break;
            case "idKeyOriginal":
                encoder
                    .setReceiver(new JsonToElasticsearchBulkIdKeyOriginal(
                                bulkParam(params), "type", "name"))
                    .setReceiver(writer());
                break;
            case "idKeyPointer":
                encoder
                    .setReceiver(new JsonToElasticsearchBulkIdKeyPointer(
                                bulkParam(params), "type", "name", true))
                    .setReceiver(writer());
                break;
            case "idPathGet":
                encoder
                    .setReceiver(new JsonToElasticsearchBulkIdPathGet(
                                bulkParam(params), "type", "name", "."))
                    .setReceiver(writer());
                break;
            case "idPathFind":
                encoder
                    .setReceiver(new JsonToElasticsearchBulkIdPathFind(
                                bulkParam(params), "type", "name", "."))
                    .setReceiver(writer());
                break;
            default:
                throw new IllegalArgumentException("Unsupported param: " + param);
        }
    }

    private String bulkParam(final String[] params) {
        if (params.length > 1) {
            return params[1];
        }
        else {
            throw new IllegalArgumentException("Invalid param: " + param);
        }
    }

    private ObjectWriter<String> writer() {
        return new ObjectWriter<>(fileName("jsonl", true));
    }

    private String fileName(final String extension, final boolean includeParam) {
        String fileName = getClass().getSimpleName().split("_")[0];

        if (includeParam) {
            try {
                fileName = fileName + "-" + URLEncoder.encode(
                        param, Charset.defaultCharset().name());
            }
            catch (java.io.UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

        return fileName + "." + extension;
    }

    private String fileName(final String extension) {
        return fileName(extension, false);
    }

}
