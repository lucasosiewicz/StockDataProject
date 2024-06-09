package com.example.bigdata;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AccessLogRecord {
    private static final Pattern logPattern = Pattern.compile("your-regex-pattern"); // Uzupełnij regexem do parsowania linii logów
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");

    private long timestampInMillis;

    public AccessLogRecord(long timestampInMillis) {
        this.timestampInMillis = timestampInMillis;
    }

    public long getTimestampInMillis() {
        return timestampInMillis;
    }

    public static boolean lineIsCorrect(String line) {
        Matcher matcher = logPattern.matcher(line);
        return matcher.matches();
    }

    public static AccessLogRecord parseFromLogLine(String line) {
        Matcher matcher = logPattern.matcher(line);
        if (matcher.matches()) {
            String timestampStr = matcher.group(1); // Zakładam, że pierwszy capture group to timestamp
            try {
                Date date = dateFormat.parse(timestampStr);
                return new AccessLogRecord(date.getTime());
            } catch (ParseException e) {
                throw new RuntimeException("Failed to parse date: " + timestampStr, e);
            }
        } else {
            throw new IllegalArgumentException("Log line is incorrect: " + line);
        }
    }
}
