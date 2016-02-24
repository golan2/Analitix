package com.analitix.utils;

import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Created by golaniz on 18/02/2016.
 */
public class ConsoleUtils {
    private static final SimpleDateFormat sdf        = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS", Locale.US);
    private static final GregorianCalendar calendar   = new GregorianCalendar(TimeZone.getTimeZone("US/Central"));

    public static void log(String s) {
        System.out.println(getCurrentDateAndTime()+"~~T["+Thread.currentThread().getName()+"] "  + s );
    }

    private static String getCurrentDateAndTime() {
        calendar.setTimeInMillis(System.currentTimeMillis());
        return sdf.format(calendar.getTime());
    }
}
