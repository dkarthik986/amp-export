package com.swift.platform.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.swift.platform.dto.ExportColumnRequest;
import com.swift.platform.dto.SearchResponse;
import lombok.RequiredArgsConstructor;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class ExportRenderService {

    private static final Set<String> DETAIL_SKIP_PATHS = Set.of("_class", "version");
    private static final List<String> DEFAULT_TARGET_KEYS = List.of("table");
    private static final int EXCEL_MAX_CELL_CHARS = 32_767;
    private static final List<String> MESSAGE_META_KEYS = List.of(
            "messageReference", "reference", "messageType", "messageFormat", "sequenceNumber", "sessionNumber", "creationDate", "section"
    );
    private static final Map<String, Field> SEARCH_RESPONSE_FIELDS = initSearchResponseFields();
    private static final DateTimeFormatter EXPORT_TS = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").withZone(ZoneOffset.UTC);

    private final ObjectMapper objectMapper;

    public Path renderChunk(Path directory,
                            String baseFileName,
                            String format,
                            List<SearchResponse> messages,
                            List<String> targetKeys,
                            List<ExportColumnRequest> columns,
                            int partIndex) throws IOException {
        String safeFormat = normalizeFormatKey(format);
        Path outputFile = directory.resolve(buildPartFileName(baseFileName, safeFormat, partIndex));

        switch (safeFormat) {
            case "csv" -> writeCsv(outputFile, buildDataset(messages, targetKeys, columns));
            case "excel" -> writeExcel(outputFile, buildDataset(messages, targetKeys, columns));
            case "json" -> writeJson(outputFile, messages, targetKeys, columns);
            case "xml" -> writeXml(outputFile, messages, targetKeys, columns);
            case "txt" -> writeText(outputFile, messages, targetKeys, columns);
            case "word" -> writeWord(outputFile, messages, targetKeys, columns);
            case "pdf" -> writePdf(outputFile, messages, targetKeys, columns);
            default -> throw new IllegalArgumentException("Unsupported export format: " + format);
        }

        return outputFile;
    }

    public String buildBaseFileName(String format, List<String> targetKeys, String scope) {
        String joinedTargets = String.join("_", normalizeTargetKeys(targetKeys))
                .replaceAll("[^a-zA-Z0-9_]+", "_")
                .toLowerCase(Locale.ROOT);
        String safeScope = safeFileToken(scope);
        return "swift_messages_" + (joinedTargets.isBlank() ? "table" : joinedTargets) + "_" + safeScope + "_" + EXPORT_TS.format(Instant.now());
    }

    public String contentTypeFor(String format) {
        return switch (normalizeFormatKey(format)) {
            case "csv" -> "text/csv;charset=UTF-8";
            case "excel" -> "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
            case "json" -> "application/json";
            case "xml" -> "application/xml";
            case "txt" -> "text/plain;charset=UTF-8";
            case "word" -> "application/msword";
            case "pdf" -> "application/pdf";
            default -> "application/octet-stream";
        };
    }

    public String extensionFor(String format) {
        return switch (normalizeFormatKey(format)) {
            case "excel" -> "xlsx";
            case "word" -> "doc";
            default -> normalizeFormatKey(format);
        };
    }

    private void writeCsv(Path outputFile, ExportDataset dataset) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
            writer.write(joinCsv(dataset.columns().stream().map(ExportColumnRequest::getLabel).toList()));
            writer.newLine();
            for (Map<String, String> row : dataset.rows()) {
                List<String> values = dataset.columns().stream()
                        .map(column -> stringify(row.get(column.getKey())))
                        .toList();
                writer.write(joinCsv(values));
                writer.newLine();
            }
        }
    }

    private void writeExcel(Path outputFile, ExportDataset dataset) throws IOException {
        try (org.apache.poi.xssf.streaming.SXSSFWorkbook workbook = new org.apache.poi.xssf.streaming.SXSSFWorkbook(250)) {
            workbook.setCompressTempFiles(true);
            var sheet = workbook.createSheet("Export");
            var headerRow = sheet.createRow(0);
            for (int i = 0; i < dataset.columns().size(); i++) {
                headerRow.createCell(i).setCellValue(toExcelCellValue(dataset.columns().get(i).getLabel()));
                sheet.setColumnWidth(i, 24 * 256);
            }
            int rowIndex = 1;
            for (Map<String, String> row : dataset.rows()) {
                var dataRow = sheet.createRow(rowIndex++);
                for (int i = 0; i < dataset.columns().size(); i++) {
                    dataRow.createCell(i).setCellValue(toExcelCellValue(stringify(row.get(dataset.columns().get(i).getKey()))));
                }
            }
            try (OutputStream outputStream = Files.newOutputStream(outputFile)) {
                workbook.write(outputStream);
            } finally {
                workbook.dispose();
            }
        }
    }

    private void writeJson(Path outputFile,
                           List<SearchResponse> messages,
                           List<String> targetKeys,
                           List<ExportColumnRequest> columns) throws IOException {
        Object payload;
        if (isTableOnly(targetKeys)) {
            ExportDataset dataset = buildDataset(messages, targetKeys, columns);
            payload = Map.of("label", dataset.label(), "columns", dataset.columns(), "rows", dataset.rows());
        } else {
            payload = buildRenderableDocuments(messages, targetKeys, columns);
        }
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(outputFile.toFile(), payload);
    }

    private void writeXml(Path outputFile,
                          List<SearchResponse> messages,
                          List<String> targetKeys,
                          List<ExportColumnRequest> columns) throws IOException {
        StringBuilder xml = new StringBuilder();
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<export>\n");
        if (isTableOnly(targetKeys)) {
            ExportDataset dataset = buildDataset(messages, targetKeys, columns);
            xml.append("  <table label=\"").append(xml(dataset.label())).append("\">\n");
            for (Map<String, String> row : dataset.rows()) {
                xml.append("    <row>\n");
                for (ExportColumnRequest column : dataset.columns()) {
                    xml.append("      <").append(xmlTag(column.getKey())).append(">")
                            .append(xml(stringify(row.get(column.getKey()))))
                            .append("</").append(xmlTag(column.getKey())).append(">\n");
                }
                xml.append("    </row>\n");
            }
            xml.append("  </table>\n");
        } else {
            for (RenderableDocument document : buildRenderableDocuments(messages, targetKeys, columns)) {
                xml.append("  <message>\n");
                xml.append("    <messageReference>").append(xml(document.messageReference())).append("</messageReference>\n");
                xml.append("    <summaryLine>").append(xml(document.summaryLine())).append("</summaryLine>\n");
                for (SectionBlock section : document.sections()) {
                    xml.append("    <section label=\"").append(xml(section.label())).append("\" type=\"").append(section.kind()).append("\">\n");
                    if ("raw".equals(section.kind())) {
                        xml.append("      <rawText>").append(xml(section.rawText())).append("</rawText>\n");
                    } else {
                        for (Map<String, String> row : section.rows()) {
                            xml.append("      <row>\n");
                            for (ExportColumnRequest column : section.columns()) {
                                xml.append("        <").append(xmlTag(column.getKey())).append(">")
                                        .append(xml(stringify(row.get(column.getKey()))))
                                        .append("</").append(xmlTag(column.getKey())).append(">\n");
                            }
                            xml.append("      </row>\n");
                        }
                    }
                    xml.append("    </section>\n");
                }
                xml.append("  </message>\n");
            }
        }
        xml.append("</export>\n");
        Files.writeString(outputFile, xml.toString(), StandardCharsets.UTF_8);
    }

    private void writeText(Path outputFile,
                           List<SearchResponse> messages,
                           List<String> targetKeys,
                           List<ExportColumnRequest> columns) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
            if (isTableOnly(targetKeys)) {
                ExportDataset dataset = buildDataset(messages, targetKeys, columns);
                writer.write(dataset.label());
                writer.newLine();
                writer.write("=".repeat(Math.max(12, dataset.label().length())));
                writer.newLine();
                writer.write(String.join(" | ", dataset.columns().stream().map(ExportColumnRequest::getLabel).toList()));
                writer.newLine();
                for (Map<String, String> row : dataset.rows()) {
                    writer.write(String.join(" | ", dataset.columns().stream()
                            .map(column -> stringify(row.get(column.getKey())).replace("\r", " ").replace("\n", " "))
                            .toList()));
                    writer.newLine();
                }
                return;
            }

            for (RenderableDocument document : buildRenderableDocuments(messages, targetKeys, columns)) {
                writer.write("Message Ref: " + document.messageReference());
                writer.newLine();
                if (!document.summaryLine().isBlank()) {
                    writer.write(document.summaryLine());
                    writer.newLine();
                }
                writer.write("-".repeat(80));
                writer.newLine();
                for (SectionBlock section : document.sections()) {
                    writer.write(section.label());
                    writer.newLine();
                    if ("raw".equals(section.kind())) {
                        writer.write(stringify(section.rawText()));
                        writer.newLine();
                    } else {
                        for (Map<String, String> row : section.rows()) {
                            if ("pair".equals(section.kind())) {
                                writer.write(stringify(row.get("field")) + ": " + stringify(row.get("value")));
                            } else {
                                List<String> parts = new ArrayList<>();
                                for (ExportColumnRequest column : section.columns()) {
                                    parts.add(column.getLabel() + "=" + stringify(row.get(column.getKey())));
                                }
                                writer.write(String.join(" | ", parts));
                            }
                            writer.newLine();
                        }
                    }
                    writer.newLine();
                }
                writer.newLine();
            }
        }
    }

    private void writeWord(Path outputFile,
                           List<SearchResponse> messages,
                           List<String> targetKeys,
                           List<ExportColumnRequest> columns) throws IOException {
        String html;
        if (isTableOnly(targetKeys)) {
            ExportDataset dataset = buildDataset(messages, targetKeys, columns);
            html = buildWordTableHtml(dataset.label(), dataset.columns(), dataset.rows());
        } else {
            html = buildWordDocumentHtml(messages, targetKeys, columns);
        }
        Files.writeString(outputFile, "\ufeff" + html, StandardCharsets.UTF_8);
    }

    private void writePdf(Path outputFile,
                          List<SearchResponse> messages,
                          List<String> targetKeys,
                          List<ExportColumnRequest> columns) throws IOException {
        try (PDDocument document = new PDDocument()) {
            PdfWriter writer = new PdfWriter(document);
            if (isTableOnly(targetKeys)) {
                ExportDataset dataset = buildDataset(messages, targetKeys, columns);
                writer.writeHeading(dataset.label());
                writer.writeLine(String.join(" | ", dataset.columns().stream().map(ExportColumnRequest::getLabel).toList()), true);
                for (Map<String, String> row : dataset.rows()) {
                    writer.writeLine(String.join(" | ", dataset.columns().stream()
                            .map(column -> stringify(row.get(column.getKey())).replace("\r", " ").replace("\n", " "))
                            .toList()), false);
                }
            } else {
                for (RenderableDocument renderable : buildRenderableDocuments(messages, targetKeys, columns)) {
                    writer.startMessage(renderable.messageReference(), renderable.summaryLine());
                    for (SectionBlock section : renderable.sections()) {
                        writer.writeSection(section);
                    }
                }
            }
            writer.finish();
            document.save(outputFile.toFile());
        }
    }

    private ExportDataset buildDataset(List<SearchResponse> messages,
                                       List<String> targetKeys,
                                       List<ExportColumnRequest> columns) {
        if (isTableOnly(targetKeys)) {
            List<ExportColumnRequest> safeColumns = normalizeColumns(columns);
            List<Map<String, String>> rows = new ArrayList<>();
            for (SearchResponse message : messages) {
                Map<String, String> row = new LinkedHashMap<>();
                for (ExportColumnRequest column : safeColumns) {
                    row.put(column.getKey(), resolveTableValue(message, column.getKey()));
                }
                rows.add(row);
            }
            return new ExportDataset("Result Table", safeColumns, rows);
        }

        List<String> orderedKeys = normalizeTargetKeys(targetKeys);
        LinkedHashSet<String> keySet = new LinkedHashSet<>(MESSAGE_META_KEYS);
        List<Map<String, String>> rows = new ArrayList<>();

        for (SearchResponse message : messages) {
            Map<String, String> meta = messageMeta(message);
            for (String targetKey : orderedKeys) {
                SectionBlock block = buildSectionBlock(message, targetKey, columns);
                for (Map<String, String> blockRow : block.rows()) {
                    Map<String, String> row = new LinkedHashMap<>(meta);
                    row.put("section", block.label());
                    row.putAll(blockRow);
                    keySet.addAll(blockRow.keySet());
                    rows.add(row);
                }
            }
        }

        List<ExportColumnRequest> mergedColumns = keySet.stream()
                .map(key -> new ExportColumnRequest(key, toPrettyLabel(key)))
                .toList();
        String label = orderedKeys.stream().map(this::targetLabel).reduce((left, right) -> left + " + " + right).orElse("Export");
        return new ExportDataset(label, mergedColumns, rows);
    }

    private List<RenderableDocument> buildRenderableDocuments(List<SearchResponse> messages,
                                                              List<String> targetKeys,
                                                              List<ExportColumnRequest> columns) {
        List<String> orderedKeys = normalizeTargetKeys(targetKeys);
        List<RenderableDocument> documents = new ArrayList<>();
        for (SearchResponse message : messages) {
            List<SectionBlock> sections = new ArrayList<>();
            for (String targetKey : orderedKeys) {
                sections.add(buildSectionBlock(message, targetKey, columns));
            }
            documents.add(new RenderableDocument(
                    firstNonBlank(message.getReference(), message.getTransactionReference(), "—"),
                    buildSummaryLine(message),
                    sections
            ));
        }
        return documents;
    }

    private SectionBlock buildSectionBlock(SearchResponse message,
                                           String targetKey,
                                           List<ExportColumnRequest> columns) {
        return switch (targetKey) {
            case "header" -> buildPairBlock("Header", getHeaderRows(message));
            case "history" -> new SectionBlock("History",
                    "table",
                    List.of(
                            new ExportColumnRequest("dateTime", "Date Time"),
                            new ExportColumnRequest("phase", "Phase"),
                            new ExportColumnRequest("action", "Action"),
                            new ExportColumnRequest("reason", "Reason"),
                            new ExportColumnRequest("entity", "Entity"),
                            new ExportColumnRequest("channel", "Channel")
                    ),
                    getHistoryRows(message),
                    null);
            case "payload" -> new SectionBlock("Extended text",
                    "table",
                    List.of(
                            new ExportColumnRequest("tag", "Tag"),
                            new ExportColumnRequest("label", "Field Label"),
                            new ExportColumnRequest("rawValue", "Raw Value")
                    ),
                    getPayloadRows(message),
                    null);
            case "rawpayload" -> new SectionBlock("Raw Payload",
                    "raw",
                    List.of(new ExportColumnRequest("rawPayload", "Raw Payload")),
                    Collections.emptyList(),
                    getRawPayloadText(message));
            case "details" -> buildPairBlock("All Fields", getDetailRows(message));
            case "table" -> {
                ExportDataset dataset = buildDataset(List.of(message), List.of("table"), columns);
                yield new SectionBlock(dataset.label(), "table", dataset.columns(), dataset.rows(), null);
            }
            default -> buildPairBlock(targetLabel(targetKey), List.of(Map.of("field", "Value", "value", "—")));
        };
    }

    private SectionBlock buildPairBlock(String label, List<Map<String, String>> rows) {
        return new SectionBlock(label,
                "pair",
                List.of(
                        new ExportColumnRequest("field", "Field"),
                        new ExportColumnRequest("value", "Value")
                ),
                rows.isEmpty() ? List.of(Map.of("field", "Field", "value", "—")) : rows,
                null);
    }

    private List<Map<String, String>> getHeaderRows(SearchResponse message) {
        List<Map<String, String>> rows = new ArrayList<>();
        addPair(rows, "Message Type", firstNonBlank(message.getType(), message.getMessageCode()));
        addPair(rows, "Protocol", firstNonBlank(message.getNetworkProtocol(), message.getService()));
        addPair(rows, "Sender", message.getSender());
        addPair(rows, "Service", message.getService());
        addPair(rows, "Receiver", message.getReceiver());
        addPair(rows, "Message Reference", message.getReference());
        addPair(rows, "Transaction Reference", message.getTransactionReference());
        return rows;
    }

    private List<Map<String, String>> getHistoryRows(SearchResponse message) {
        List<Map<String, Object>> rawRows = message.getHistoryLines() == null ? Collections.emptyList() : message.getHistoryLines();
        List<Map<String, String>> rows = new ArrayList<>();
        for (Map<String, Object> rawRow : rawRows) {
            Map<String, String> row = new LinkedHashMap<>();
            row.put("dateTime", stringify(rawRow.get("historyDate")));
            row.put("phase", stringify(rawRow.get("phase")));
            row.put("action", stringify(rawRow.get("action")));
            row.put("reason", stringify(rawRow.get("reason")));
            row.put("entity", stringify(rawRow.get("entity")));
            row.put("channel", stringify(rawRow.get("channel")));
            rows.add(row);
        }
        return rows;
    }

    private List<Map<String, String>> getPayloadRows(SearchResponse message) {
        List<Map<String, Object>> rawRows = message.getBlock4Fields() == null ? Collections.emptyList() : message.getBlock4Fields();
        List<Map<String, String>> rows = new ArrayList<>();
        for (Map<String, Object> rawRow : rawRows) {
            Map<String, String> row = new LinkedHashMap<>();
            String tag = stringify(rawRow.get("tag"));
            String label = stringify(rawRow.get("label"));
            row.put("tag", tag);
            row.put("label", !label.isBlank() ? label : (!tag.isBlank() ? "Tag " + tag : "—"));
            row.put("rawValue", stringify(rawRow.get("rawValue")));
            rows.add(row);
        }
        return rows;
    }

    private List<Map<String, String>> getDetailRows(SearchResponse message) {
        List<Map<String, String>> rows = new ArrayList<>();
        flattenObjectFields(message.getRawMessage(), "", rows);
        return rows;
    }

    private void flattenObjectFields(Map<String, Object> source, String prefix, List<Map<String, String>> out) {
        if (source == null || source.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String key = entry.getKey();
            String path = prefix.isBlank() ? key : prefix + "." + key;
            if (DETAIL_SKIP_PATHS.contains(key) || DETAIL_SKIP_PATHS.contains(path)) {
                continue;
            }
            Object value = unwrapMetaObject(entry.getValue());
            if (value == null || "".equals(value)) {
                continue;
            }
            if (value instanceof Map<?, ?> nested) {
                Map<String, Object> converted = new LinkedHashMap<>();
                nested.forEach((nestedKey, nestedValue) -> converted.put(String.valueOf(nestedKey), nestedValue));
                flattenObjectFields(converted, path, out);
                continue;
            }
            addPair(out, toPrettyLabel(path), stringify(value));
        }
    }

    private Object unwrapMetaObject(Object value) {
        if (!(value instanceof Map<?, ?> map)) {
            return value;
        }
        if (map.size() == 1) {
            if (map.containsKey("$oid")) return map.get("$oid");
            if (map.containsKey("$date")) return map.get("$date");
            if (map.containsKey("$numberLong")) return map.get("$numberLong");
        }
        return value;
    }

    private Map<String, String> messageMeta(SearchResponse message) {
        Map<String, String> meta = new LinkedHashMap<>();
        meta.put("messageReference", stringify(message.getReference()));
        meta.put("reference", stringify(message.getReference()));
        meta.put("messageType", firstNonBlank(message.getType(), message.getMessageCode(), message.getMessageType()));
        meta.put("messageFormat", firstNonBlank(message.getFormat(), message.getMessageFormat(), message.getMessageType()));
        meta.put("sequenceNumber", message.getSequenceNumber() == null ? "" : String.valueOf(message.getSequenceNumber()));
        meta.put("sessionNumber", stringify(message.getSessionNumber()));
        meta.put("creationDate", stringify(message.getCreationDate()));
        return meta;
    }

    private String buildSummaryLine(SearchResponse message) {
        List<String> parts = new ArrayList<>();
        String type = firstNonBlank(message.getType(), message.getMessageCode());
        String format = firstNonBlank(message.getFormat(), message.getMessageFormat(), message.getMessageType());
        String direction = formatDirection(firstNonBlank(message.getIo(), message.getDirection()));
        if (!type.isBlank()) parts.add(type);
        if (!format.isBlank()) parts.add(format);
        if (!direction.isBlank()) parts.add(direction);
        return String.join("   ", parts);
    }

    private String getRawPayloadText(SearchResponse message) {
        if (message.getRawFin() != null && !message.getRawFin().isBlank()) {
            return message.getRawFin();
        }
        Map<String, Object> rawMessage = message.getRawMessage();
        if (rawMessage == null) {
            return "—";
        }
        Object mtPayload = rawMessage.get("mtPayload");
        if (mtPayload instanceof Map<?, ?> map) {
            Object rawFin = map.get("rawFin");
            if (rawFin != null) {
                return stringify(rawFin);
            }
        }
        return "—";
    }

    private String resolveTableValue(SearchResponse response, String key) {
        if (response == null || key == null || key.isBlank()) {
            return "";
        }
        return switch (key) {
            case "reference" -> stringify(firstNonBlank(response.getReference(), response.getTransactionReference()));
            case "format" -> normalizeDisplayFormat(firstNonBlank(response.getFormat(), response.getMessageType()));
            case "type" -> stringify(firstNonBlank(response.getType(), response.getMessageCode()));
            case "direction" -> formatDirection(firstNonBlank(response.getDirection(), response.getIo()));
            default -> stringify(readSearchResponseField(response, key));
        };
    }

    private Object readSearchResponseField(SearchResponse response, String key) {
        Field field = SEARCH_RESPONSE_FIELDS.get(key);
        if (field == null) {
            return null;
        }
        try {
            return field.get(response);
        } catch (IllegalAccessException ignored) {
            return null;
        }
    }

    private static Map<String, Field> initSearchResponseFields() {
        Map<String, Field> fields = new LinkedHashMap<>();
        for (Field field : SearchResponse.class.getDeclaredFields()) {
            field.setAccessible(true);
            fields.put(field.getName(), field);
        }
        return Map.copyOf(fields);
    }

    private String normalizeDisplayFormat(String rawFormat) {
        return rawFormat == null ? "" : rawFormat.replace("ALL-MT&MX", "ALL MT&MX");
    }

    private boolean isTableOnly(List<String> targetKeys) {
        List<String> normalized = normalizeTargetKeys(targetKeys);
        return normalized.size() == 1 && "table".equals(normalized.get(0));
    }

    private List<String> normalizeTargetKeys(List<String> targetKeys) {
        if (targetKeys == null || targetKeys.isEmpty()) {
            return DEFAULT_TARGET_KEYS;
        }
        List<String> normalized = new ArrayList<>();
        for (String key : targetKeys) {
            if (key == null || key.isBlank()) {
                continue;
            }
            normalized.add(key.trim().toLowerCase(Locale.ROOT));
        }
        return normalized.isEmpty() ? DEFAULT_TARGET_KEYS : normalized;
    }

    private List<ExportColumnRequest> normalizeColumns(List<ExportColumnRequest> columns) {
        List<ExportColumnRequest> safe = new ArrayList<>();
        if (columns != null) {
            for (ExportColumnRequest column : columns) {
                if (column == null || column.getKey() == null || column.getKey().isBlank()) continue;
                String key = column.getKey().trim();
                String label = column.getLabel() == null || column.getLabel().isBlank() ? toPrettyLabel(key) : column.getLabel().trim();
                safe.add(new ExportColumnRequest(key, label));
            }
        }
        if (safe.isEmpty()) {
            safe.add(new ExportColumnRequest("reference", "Reference"));
        }
        return safe;
    }

    private String normalizeFormatKey(String format) {
        return format == null ? "" : format.trim().toLowerCase(Locale.ROOT);
    }

    private String buildPartFileName(String baseFileName, String format, int partIndex) {
        return baseFileName + String.format("_part_%03d.%s", Math.max(1, partIndex), extensionFor(format));
    }

    private String joinCsv(List<String> values) {
        return String.join(",", values.stream().map(this::escapeCsvCell).toList());
    }

    private String escapeCsvCell(String value) {
        String safe = stringify(value).replace("\"", "\"\"");
        return "\"" + safe + "\"";
    }

    private String targetLabel(String key) {
        return switch (key) {
            case "table" -> "Result Table";
            case "header" -> "Header";
            case "rawpayload" -> "Raw Payload";
            case "payload" -> "Extended text";
            case "history" -> "History";
            case "details" -> "All Fields";
            default -> toPrettyLabel(key);
        };
    }

    private void addPair(List<Map<String, String>> rows, String field, Object value) {
        String safeValue = stringify(value);
        if (safeValue.isBlank()) {
            return;
        }
        rows.add(Map.of("field", field, "value", safeValue));
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "";
    }

    private String formatDirection(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        String normalized = value.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "I" -> "INCOMING";
            case "O" -> "OUTGOING";
            default -> normalized;
        };
    }

    private String stringify(Object value) {
        if (value == null) return "";
        if (value instanceof String string) return string;
        if (value instanceof Number || value instanceof Boolean) return String.valueOf(value);
        if (value instanceof Instant instant) return instant.toString();
        if (value instanceof List<?> list) return list.stream().map(this::stringify).reduce((left, right) -> left + ", " + right).orElse("");
        return String.valueOf(value);
    }

    private String toExcelCellValue(String value) {
        if (value == null) {
            return "";
        }
        return value.length() <= EXCEL_MAX_CELL_CHARS ? value : value.substring(0, EXCEL_MAX_CELL_CHARS);
    }

    private String safeFileToken(String raw) {
        if (raw == null || raw.isBlank()) return "all";
        return raw.replaceAll("[^a-zA-Z0-9]+", "_").replaceAll("_+", "_").replaceAll("^_|_$", "").toLowerCase(Locale.ROOT);
    }

    private String toPrettyLabel(String key) {
        if (key == null || key.isBlank()) return "";
        String spaced = key.replaceAll("([a-z])([A-Z])", "$1 $2")
                .replaceAll("[._-]+", " ")
                .trim();
        StringBuilder out = new StringBuilder();
        boolean capitalize = true;
        for (char ch : spaced.toCharArray()) {
            if (capitalize && Character.isLetter(ch)) {
                out.append(Character.toUpperCase(ch));
                capitalize = false;
            } else {
                out.append(ch);
                capitalize = Character.isWhitespace(ch);
            }
        }
        return out.toString();
    }

    private String xml(String raw) {
        return stringify(raw)
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&apos;");
    }

    private String xmlTag(String raw) {
        String safe = safeFileToken(raw);
        return safe.isBlank() ? "value" : safe;
    }

    private String html(String raw) {
        return xml(raw).replace("\r\n", "<br />").replace("\n", "<br />");
    }

    private String buildWordTableHtml(String title, List<ExportColumnRequest> columns, List<Map<String, String>> rows) {
        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html><html><head><meta charset=\"UTF-8\" /><title>")
                .append(xml(title))
                .append("</title><style>")
                .append("body{font-family:Arial,sans-serif;padding:18px;color:#111827;}h2{margin:0 0 12px;font-size:18px;}")
                .append("table{border-collapse:collapse;width:100%;font-size:10.5pt;}th,td{border:1px solid #d1d5db;padding:6px 8px;vertical-align:top;text-align:left;}th{background:#f3f4f6;font-weight:700;}")
                .append("</style></head><body><h2>").append(xml(title)).append("</h2><table><thead><tr>");
        for (ExportColumnRequest column : columns) {
            html.append("<th>").append(xml(column.getLabel())).append("</th>");
        }
        html.append("</tr></thead><tbody>");
        for (Map<String, String> row : rows) {
            html.append("<tr>");
            for (ExportColumnRequest column : columns) {
                html.append("<td>").append(html(row.get(column.getKey()))).append("</td>");
            }
            html.append("</tr>");
        }
        html.append("</tbody></table></body></html>");
        return html.toString();
    }

    private String buildWordDocumentHtml(List<SearchResponse> messages,
                                         List<String> targetKeys,
                                         List<ExportColumnRequest> columns) {
        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html><html><head><meta charset=\"UTF-8\" /><title>SWIFT Export</title><style>")
                .append("body{font-family:Arial,sans-serif;padding:18px;color:#111827;} .message{page-break-before:always;} .message:first-child{page-break-before:auto;}")
                .append("h1{font-size:20px;margin:0 0 8px;} h2{font-size:15px;margin:18px 0 8px;color:#1e3a8a;} .summary{font-size:11px;color:#64748b;margin-bottom:10px;}")
                .append("table{border-collapse:collapse;width:100%;font-size:10pt;}th,td{border:1px solid #d1d5db;padding:6px 8px;vertical-align:top;text-align:left;}th{background:#f3f4f6;font-weight:700;}pre{background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:12px;white-space:pre-wrap;font-family:'Courier New',monospace;font-size:9pt;}")
                .append("</style></head><body>");

        for (RenderableDocument document : buildRenderableDocuments(messages, targetKeys, columns)) {
            html.append("<section class=\"message\">")
                    .append("<h1>Message Ref: ").append(xml(document.messageReference())).append("</h1>")
                    .append("<div class=\"summary\">").append(html(document.summaryLine())).append("</div>");
            for (SectionBlock section : document.sections()) {
                html.append("<h2>").append(xml(section.label())).append("</h2>");
                if ("raw".equals(section.kind())) {
                    html.append("<pre>").append(html(section.rawText())).append("</pre>");
                    continue;
                }
                html.append("<table><thead><tr>");
                for (ExportColumnRequest column : section.columns()) {
                    html.append("<th>").append(xml(column.getLabel())).append("</th>");
                }
                html.append("</tr></thead><tbody>");
                for (Map<String, String> row : section.rows()) {
                    html.append("<tr>");
                    for (ExportColumnRequest column : section.columns()) {
                        html.append("<td>").append(html(row.get(column.getKey()))).append("</td>");
                    }
                    html.append("</tr>");
                }
                html.append("</tbody></table>");
            }
            html.append("</section>");
        }

        html.append("</body></html>");
        return html.toString();
    }

    private record ExportDataset(String label, List<ExportColumnRequest> columns, List<Map<String, String>> rows) {}

    private record SectionBlock(String label,
                                String kind,
                                List<ExportColumnRequest> columns,
                                List<Map<String, String>> rows,
                                String rawText) {}

    private record RenderableDocument(String messageReference,
                                      String summaryLine,
                                      List<SectionBlock> sections) {}

    private static final class PdfWriter {
        private static final float PAGE_MARGIN = 42f;
        private static final float PAGE_WIDTH = PDRectangle.A4.getWidth();
        private static final float PAGE_HEIGHT = PDRectangle.A4.getHeight();
        private static final float CONTENT_WIDTH = PAGE_WIDTH - (PAGE_MARGIN * 2);

        private final PDDocument document;
        private PDPageContentStream stream;
        private float cursorY;

        private PdfWriter(PDDocument document) throws IOException {
            this.document = document;
            newPage();
        }

        private void startMessage(String messageReference, String summaryLine) throws IOException {
            ensureSpace(40f);
            writeWrapped("Message Ref: " + messageReference, PDType1Font.HELVETICA_BOLD, 12f, true);
            if (summaryLine != null && !summaryLine.isBlank()) {
                writeWrapped(summaryLine, PDType1Font.HELVETICA, 8.5f, false);
            }
            drawDivider();
        }

        private void writeHeading(String heading) throws IOException {
            writeWrapped(heading, PDType1Font.HELVETICA_BOLD, 12f, true);
            drawDivider();
        }

        private void writeSection(SectionBlock section) throws IOException {
            writeWrapped(section.label(), PDType1Font.HELVETICA_BOLD, 10f, true);
            if ("raw".equals(section.kind())) {
                writeWrapped(section.rawText(), PDType1Font.COURIER, 7.5f, false);
                cursorY -= 6f;
                return;
            }
            for (Map<String, String> row : section.rows()) {
                String line;
                if ("pair".equals(section.kind())) {
                    line = stringifyStatic(row.get("field")) + ": " + stringifyStatic(row.get("value"));
                } else {
                    List<String> pieces = new ArrayList<>();
                    for (ExportColumnRequest column : section.columns()) {
                        pieces.add(column.getLabel() + ": " + stringifyStatic(row.get(column.getKey())));
                    }
                    line = String.join(" | ", pieces);
                }
                writeWrapped(line, PDType1Font.HELVETICA, 8f, false);
            }
            cursorY -= 6f;
        }

        private void writeLine(String line, boolean bold) throws IOException {
            writeWrapped(line, bold ? PDType1Font.HELVETICA_BOLD : PDType1Font.HELVETICA, bold ? 10f : 8f, false);
        }

        private void drawDivider() throws IOException {
            ensureSpace(14f);
            stream.moveTo(PAGE_MARGIN, cursorY);
            stream.lineTo(PAGE_WIDTH - PAGE_MARGIN, cursorY);
            stream.stroke();
            cursorY -= 12f;
        }

        private void writeWrapped(String text, PDType1Font font, float fontSize, boolean extraGap) throws IOException {
            List<String> lines = wrapText(text, font, fontSize, CONTENT_WIDTH);
            for (String line : lines) {
                ensureSpace(fontSize + 6f);
                stream.beginText();
                stream.setFont(font, fontSize);
                stream.newLineAtOffset(PAGE_MARGIN, cursorY);
                stream.showText(line);
                stream.endText();
                cursorY -= fontSize + 4f;
            }
            if (extraGap) {
                cursorY -= 4f;
            }
        }

        private void ensureSpace(float neededHeight) throws IOException {
            if (cursorY - neededHeight >= PAGE_MARGIN) {
                return;
            }
            newPage();
        }

        private void newPage() throws IOException {
            closeStream();
            PDPage page = new PDPage(PDRectangle.A4);
            document.addPage(page);
            stream = new PDPageContentStream(document, page);
            cursorY = PAGE_HEIGHT - PAGE_MARGIN;
        }

        private void closeStream() throws IOException {
            if (stream != null) {
                stream.close();
                stream = null;
            }
        }

        private void finish() throws IOException {
            closeStream();
        }

        private List<String> wrapText(String text, PDType1Font font, float fontSize, float maxWidth) throws IOException {
            List<String> wrapped = new ArrayList<>();
            String safe = text == null || text.isBlank() ? "—" : text;
            for (String rawLine : safe.replace("\r", "").split("\n")) {
                String line = rawLine.isEmpty() ? " " : rawLine;
                StringBuilder current = new StringBuilder();
                for (String word : line.split("\\s+")) {
                    String candidate = current.isEmpty() ? word : current + " " + word;
                    float width = font.getStringWidth(candidate) / 1000f * fontSize;
                    if (width > maxWidth && !current.isEmpty()) {
                        wrapped.add(current.toString());
                        current = new StringBuilder(word);
                    } else {
                        current = new StringBuilder(candidate);
                    }
                }
                wrapped.add(current.isEmpty() ? " " : current.toString());
            }
            return wrapped;
        }

        private static String stringifyStatic(String value) {
            return value == null ? "" : value;
        }
    }
}
